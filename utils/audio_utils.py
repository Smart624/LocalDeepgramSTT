from pathlib import Path
from pydub import AudioSegment
from moviepy.editor import VideoFileClip
import subprocess
import logging

logger = logging.getLogger(__name__)
CHUNK_SIZE = 15 * 60  # 15 minutes in seconds

def extract_audio_from_video(video_path: Path) -> Path:
    logger.info(f"Extracting audio from video file: {video_path}")
    audio_path = video_path.with_suffix('.aac')
    try:
        command = [
            'ffmpeg',
            '-i', str(video_path),
            '-c:a', 'aac',
            '-b:a', '96k',
            '-vn',
            str(audio_path)
        ]
        subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
            command = [
                'ffmpeg',
                '-i', str(file_path),
                '-c:a', 'aac',
                '-b:a', '96k',
                str(aac_path)
            ]
            subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            file_path = aac_path
            logger.info(f"Converted file to AAC: {file_path}")

        # Get duration of the audio file
        probe_command = [
            'ffprobe',
            '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            str(file_path)
        ]
        duration = float(subprocess.check_output(probe_command).decode('utf-8').strip())

        if duration <= CHUNK_SIZE:
            logger.info(f"File {file_path} is smaller than or equal to chunk size. Using whole file.")
            return [file_path]

        chunks = []
        for i in range(0, int(duration), CHUNK_SIZE):
            chunk_path = file_path.with_name(f"{file_path.stem}_chunk_{i//CHUNK_SIZE}.aac")
            chunk_command = [
                'ffmpeg',
                '-i', str(file_path),
                '-ss', str(i),
                '-t', str(CHUNK_SIZE),
                '-c', 'copy',
                str(chunk_path)
            ]
            subprocess.run(chunk_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            chunks.append(chunk_path)
            logger.info(f"Created chunk: {chunk_path}")

        logger.info(f"Created {len(chunks)} chunks for {file_path}")
        return chunks
    except Exception as e:
        logger.error(f"Error processing audio file {file_path}: {e}")
        raise