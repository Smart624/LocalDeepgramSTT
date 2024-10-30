import os
import subprocess
import logging
from pathlib import Path
from tqdm import tqdm
from datetime import timedelta
import json

logger = logging.getLogger(__name__)
CHUNK_SIZE = 15 * 60  # 15 minutes in seconds
BUFFER_SIZE = 64 * 1024  # 64KB buffer for file operations

def format_duration(seconds):
    """Convert seconds to HH:MM:SS format"""
    return str(timedelta(seconds=int(seconds)))

def get_duration(file_path, check_video=False):
    """Get media duration with fallback methods but never fail"""
    try:
        file_size = os.path.getsize(file_path)
        command = [
            'ffprobe',
            '-v', 'error',
            '-select_streams', 'a:0',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            str(file_path)
        ]
        output = subprocess.check_output(command, stderr=subprocess.DEVNULL).decode('utf-8').strip()
        duration = float(output)
        
        if check_video:
            logger.info(f"Video file: {file_path.name}")
            logger.info(f"Duration: {format_duration(duration)} ({duration:.2f} seconds)")
            logger.info(f"File size: {file_size / (1024*1024):.2f} MB")
            return duration, file_size
            
        return duration
    except:
        # Fallback to file size estimation
        file_size = os.path.getsize(file_path)
        estimated_duration = (file_size * 8) / (128 * 1000)  # Assume 128kbps
        return estimated_duration

def is_valid_audio(file_path, min_size_kb=10):
    """Validate audio chunk without relying on duration"""
    try:
        # Check file size
        file_size = os.path.getsize(file_path) / 1024  # Size in KB
        if file_size < min_size_kb:
            return False
            
        # Quick check if file is readable
        command = [
            'ffprobe',
            '-v', 'error',
            '-select_streams', 'a:0',
            '-show_entries', 'stream=codec_type',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            str(file_path)
        ]
        output = subprocess.run(command, capture_output=True, text=True)
        return output.stdout.strip() == 'audio'
    except:
        return False

def extract_audio_from_video(video_path: Path) -> Path:
    """Extract audio while attempting to preserve correct duration metadata"""
    logger.info(f"Starting audio extraction from: {video_path}")
    audio_path = video_path.with_suffix('.aac')
    temp_path = video_path.with_suffix('.temp.aac')
    
    try:
        video_duration, video_size = get_duration(video_path, check_video=True)
        
        # First extraction
        command = [
            'ffmpeg',
            '-y',
            '-i', str(video_path),
            '-map', '0:a:0',
            '-c:a', 'aac',
            '-b:a', '128k',
            '-movflags', '+faststart',
            # Try to force duration in initial extraction
            '-metadata', f'duration={video_duration}',
            '-threads', str(os.cpu_count()),
            str(temp_path)
        ]
        
        process = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=BUFFER_SIZE
        )
        
        pbar = tqdm(total=100, desc="Extracting Audio", unit="%")
        last_progress = 0
        
        while True:
            line = process.stderr.readline()
            if not line:
                break
            
            if "time=" in line:
                try:
                    time_str = line.split("time=")[1].split()[0]
                    hours, minutes, seconds = time_str.split(':')
                    current_time = float(hours) * 3600 + float(minutes) * 60 + float(seconds)
                    progress = min(int((current_time / video_duration) * 100), 100)
                    if progress > last_progress:
                        pbar.update(progress - last_progress)
                        last_progress = progress
                except:
                    continue
        
        pbar.close()
        process.wait()
        
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, command)
        
        # Try to correct duration metadata in a second pass
        try:
            # Get current audio duration
            audio_duration = get_duration(temp_path)
            logger.info(f"Initial extracted audio duration: {format_duration(audio_duration)} ({audio_duration:.2f} seconds)")
            
            if abs(audio_duration - video_duration) > video_duration * 0.01:  # 1% tolerance
                logger.info(f"Attempting to correct audio duration metadata to match video: {format_duration(video_duration)}")
                
                correction_command = [
                    'ffmpeg',
                    '-y',
                    '-i', str(temp_path),
                    '-c', 'copy',
                    '-metadata', f'duration={video_duration}',
                    '-metadata', f'length={int(video_duration * 1000)}',  # milliseconds
                    '-movflags', '+faststart',
                    str(audio_path)
                ]
                
                subprocess.run(correction_command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                os.remove(temp_path)
                
                # Verify correction
                final_duration = get_duration(audio_path)
                logger.info(f"Final audio duration after correction: {format_duration(final_duration)} ({final_duration:.2f} seconds)")
            else:
                # Duration is correct, just rename the file
                os.replace(temp_path, audio_path)
        except Exception as e:
            logger.warning(f"Failed to correct duration metadata: {e}")
            # If correction fails, use the original extracted file
            if os.path.exists(temp_path):
                os.replace(temp_path, audio_path)
            if not os.path.exists(audio_path):
                raise Exception("Failed to produce output audio file")
        
        # Final duration check (informational only)
        try:
            final_duration = get_duration(audio_path)
            logger.info(f"Extracted audio duration: {format_duration(final_duration)} ({final_duration:.2f} seconds)")
            if final_duration > video_duration * 2:
                logger.warning(f"Reported audio duration seems incorrect - will rely on chunk detection")
        except:
            logger.warning("Could not determine final audio duration - will rely on chunk detection")
        
        return audio_path
    except Exception as e:
        logger.error(f"Failed to extract audio: {e}")
        for path in [audio_path, temp_path]:
            if path.exists():
                try:
                    os.remove(path)
                except:
                    pass
        raise

def chunk_audio(file_path: Path) -> list:
    """Chunk audio while handling any duration issues gracefully"""
    file_path = file_path.resolve()
    logger.info(f"Processing audio file: {file_path}")
    
    try:
        # Convert to AAC if needed
        if file_path.suffix.lower() != '.aac':
            aac_path = file_path.with_suffix('.aac')
            command = [
                'ffmpeg',
                '-y',
                '-i', str(file_path),
                '-c:a', 'aac',
                '-b:a', '128k',
                '-movflags', '+faststart',
                '-threads', str(os.cpu_count()),
                str(aac_path)
            ]
            
            subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            file_path = aac_path
        
        # Get duration but don't trust it completely
        duration = get_duration(file_path)
        logger.info(f"Starting chunking process for {format_duration(duration)} audio")
        
        # Calculate expected chunks but prepare for more or less
        total_chunks = max((int(duration) + CHUNK_SIZE - 1) // CHUNK_SIZE, 1)
        logger.info(f"Expected chunks: {total_chunks}")
        
        chunks = []
        invalid_count = 0
        max_invalid = 3  # Stop after 3 consecutive invalid chunks
        chunk_num = 0
        
        with tqdm(total=None, desc="Creating chunks", unit="chunk") as pbar:
            while True:
                if invalid_count >= max_invalid:
                    logger.info(f"Stopping chunk creation after {max_invalid} consecutive invalid chunks")
                    break
                
                start_time = chunk_num * CHUNK_SIZE
                chunk_path = file_path.with_name(f"{file_path.stem}_chunk_{chunk_num}.aac")
                
                try:
                    command = [
                        'ffmpeg',
                        '-y',
                        '-ss', str(start_time),
                        '-t', str(CHUNK_SIZE),
                        '-i', str(file_path),
                        '-c', 'copy',  # Use stream copy for faster processing
                        '-movflags', '+faststart',
                        str(chunk_path)
                    ]
                    
                    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    
                    if is_valid_audio(chunk_path):
                        chunk_duration = get_duration(chunk_path)
                        if chunk_duration > 0:
                            chunks.append(chunk_path)
                            invalid_count = 0
                            logger.info(f"Chunk {len(chunks)}: {format_duration(chunk_duration)}")
                        else:
                            raise ValueError("Invalid chunk duration")
                    else:
                        raise ValueError("Invalid audio chunk")
                        
                except Exception as e:
                    invalid_count += 1
                    logger.warning(f"Failed chunk {chunk_num + 1}: {str(e)}")
                    try:
                        os.remove(chunk_path)
                    except:
                        pass
                
                chunk_num += 1
                pbar.update(1)
        
        # Log final results
        total_chunk_duration = sum(get_duration(chunk) for chunk in chunks)
        logger.info(f"Created {len(chunks)} chunks, total duration: {format_duration(total_chunk_duration)}")
        return chunks
        
    except Exception as e:
        logger.error(f"Error in chunking process: {e}")
        raise

def cleanup_audio_files(files: list):
    """Clean up temporary files"""
    for file in files:
        try:
            if os.path.exists(file):
                os.remove(file)
                logger.info(f"Cleaned up: {file}")
        except Exception as e:
            logger.warning(f"Failed to clean up {file}: {e}")