import os
import subprocess
import logging
from pathlib import Path
from tqdm import tqdm

logger = logging.getLogger(__name__)
CHUNK_SIZE = 15 * 60  # 15 minutes in seconds

def get_duration(file_path):
    command = [
        'ffprobe',
        '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        str(file_path)
    ]
    output = subprocess.check_output(command).decode('utf-8').strip()
    return float(output)

def is_valid_audio(file_path):
    try:
        duration = get_duration(file_path)
        return duration > 0
    except:
        return False

def extract_audio_from_video(video_path: Path) -> Path:
    logger.info(f"Extracting audio from video file: {video_path}")
    audio_path = video_path.with_suffix('.aac')
    try:
        # Get video duration for progress bar
        duration = get_duration(video_path)
        
        command = [
            'ffmpeg',
            '-hwaccel', 'auto',  # Use GPU acceleration if available
            '-i', str(video_path),
            '-c:a', 'aac',
            '-b:a', '128k',
            '-vn',
            '-threads', str(os.cpu_count()),  # Use all available CPU cores
            str(audio_path)
        ]
        
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        
        # Set up progress bar
        pbar = tqdm(total=100, desc="Extracting Audio", unit="%")
        
        for line in process.stderr:
            if "time=" in line:
                time_str = line.split("time=")[1].split()[0]
                hours, minutes, seconds = time_str.split(':')
                current_time = float(hours) * 3600 + float(minutes) * 60 + float(seconds)
                progress = min(int((current_time / duration) * 100), 100)
                pbar.update(progress - pbar.n)
        
        pbar.close()
        process.wait()
        
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, command)
        
        logger.info(f"Extracted audio saved to {audio_path}")
        return audio_path
    except Exception as e:
        logger.error(f"Failed to extract audio from video {video_path}: {e}")
        raise

def chunk_audio(file_path: Path) -> list:
    file_path = file_path.resolve()
    logger.info(f"Chunking audio file: {file_path}")

    try:
        # Convert to AAC if not already
        if file_path.suffix.lower() != '.aac':
            aac_path = file_path.with_suffix('.aac')
            duration = get_duration(file_path)
            
            command = [
                'ffmpeg',
                '-hwaccel', 'auto',  # Use GPU acceleration if available
                '-i', str(file_path),
                '-c:a', 'aac',
                '-b:a', '128k',
                '-threads', str(os.cpu_count()),  # Use all available CPU cores
                str(aac_path)
            ]
            
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            
            # Set up progress bar
            pbar = tqdm(total=100, desc="Converting to AAC", unit="%")
            
            for line in process.stderr:
                if "time=" in line:
                    time_str = line.split("time=")[1].split()[0]
                    hours, minutes, seconds = time_str.split(':')
                    current_time = float(hours) * 3600 + float(minutes) * 60 + float(seconds)
                    progress = min(int((current_time / duration) * 100), 100)
                    pbar.update(progress - pbar.n)
            
            pbar.close()
            process.wait()
            
            if process.returncode != 0:
                raise subprocess.CalledProcessError(process.returncode, command)
            
            file_path = aac_path
            logger.info(f"Converted file to AAC: {file_path}")

        # Get duration of the audio file
        duration = get_duration(file_path)

        if duration <= CHUNK_SIZE:
            logger.info(f"File {file_path} is smaller than or equal to chunk size. Using whole file.")
            return [file_path] if is_valid_audio(file_path) else []

        chunks = []
        for i in range(0, int(duration), CHUNK_SIZE):
            chunk_path = file_path.with_name(f"{file_path.stem}_chunk_{i//CHUNK_SIZE}.aac")
            end_time = min(i + CHUNK_SIZE, duration)
            chunk_duration = end_time - i
            
            if chunk_duration < 1:  # Skip chunks shorter than 1 second
                continue
            
            chunk_command = [
                'ffmpeg',
                '-hwaccel', 'auto',  # Use GPU acceleration if available
                '-i', str(file_path),
                '-ss', str(i),
                '-t', str(chunk_duration),
                '-c', 'copy',
                '-threads', str(os.cpu_count()),  # Use all available CPU cores
                str(chunk_path)
            ]
            subprocess.run(chunk_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            if is_valid_audio(chunk_path):
                chunks.append(chunk_path)
                logger.info(f"Created valid chunk: {chunk_path}")
            else:
                logger.warning(f"Created invalid chunk: {chunk_path}. Skipping.")
                os.remove(chunk_path)

        logger.info(f"Created {len(chunks)} valid chunks for {file_path}")
        return chunks
    except Exception as e:
        logger.error(f"Error processing audio file {file_path}: {e}")
        raise

def cleanup_audio_files(files: list):
    for file in files:
        try:
            os.remove(file)
            logger.info(f"Deleted audio file: {file}")
        except Exception as e:
            logger.warning(f"Failed to delete audio file {file}: {e}")