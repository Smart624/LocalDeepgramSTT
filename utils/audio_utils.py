from pathlib import Path
from pydub import AudioSegment
from moviepy.editor import VideoFileClip
import logging

logger = logging.getLogger(__name__)
CHUNK_SIZE = 15 * 60  # 15 minutes in seconds

def extract_audio_from_video(video_path: Path) -> Path:
    logger.info(f"Extracting audio from video file: {video_path}")
    audio_path = video_path.with_suffix('.wav')
    try:
        with VideoFileClip(str(video_path)) as video:
            audio = video.audio
            audio.write_audiofile(str(audio_path))
        logger.info(f"Extracted audio saved to {audio_path}")
        return audio_path
    except Exception as e:
        logger.error(f"Failed to extract audio from video {video_path}: {e}")
        raise

def chunk_audio(file_path: Path) -> list:
    file_path = file_path.resolve()
    logger.info(f"Chunking audio file: {file_path}")

    try:
        # Convert to WAV if not already
        if file_path.suffix.lower() != '.wav':
            audio = AudioSegment.from_file(str(file_path))
            wav_path = file_path.with_suffix('.wav')
            audio.export(str(wav_path), format="wav")
            file_path = wav_path
            logger.info(f"Converted file to WAV: {file_path}")

        audio = AudioSegment.from_wav(str(file_path))
    except Exception as e:
        logger.error(f"Error loading audio file: {e}")
        raise

    duration_ms = len(audio)
    chunk_size_ms = CHUNK_SIZE * 1000  # Convert to milliseconds

    if duration_ms <= chunk_size_ms:
        logger.info(f"File {file_path} is smaller than chunk size. Using whole file.")
        return [file_path]

    chunks = []
    for i in range(0, duration_ms, chunk_size_ms):
        chunk = audio[i:i+chunk_size_ms]
        chunk_path = file_path.with_name(f"{file_path.stem}_chunk_{i//chunk_size_ms}.wav")
        logger.info(f"Exporting chunk to {chunk_path}")
        try:
            chunk.export(str(chunk_path), format="wav")
            chunks.append(chunk_path)
        except Exception as e:
            logger.error(f"Error exporting chunk {i//chunk_size_ms}: {e}")
            raise

    logger.info(f"Created {len(chunks)} chunks for {file_path}")
    return chunks
