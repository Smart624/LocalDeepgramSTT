import os
import asyncio
import logging
from pathlib import Path
from typing import List, Dict
from deepgram import DeepgramClient, PrerecordedOptions
import aiofiles
import backoff
from pydub import AudioSegment
from utils.audio_utils import extract_audio_from_video, chunk_audio
import unicodedata
from tqdm import tqdm

logger = logging.getLogger(__name__)

class AudioTranscriber:
    def __init__(self, config):
        self.config = config
        self.semaphore = asyncio.Semaphore(config.max_concurrent_tasks)
        self.deepgram = DeepgramClient(os.getenv('DG_API_KEY'))

    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    async def transcribe_chunk(self, chunk_path: Path) -> dict:
        try:
            with open(chunk_path, "rb") as audio:
                source = {"buffer": audio, "mimetype": "audio/wav"}
                options = PrerecordedOptions(
                    model="nova-2",
                    smart_format=True,
                    language=self.config.language,
                )
                response = await self.deepgram.listen.prerecorded.v("1").transcribe_file(source, options)
                return response
        except Exception as e:
            logger.error(f"Failed to transcribe chunk {chunk_path}: {e}")
            raise

    async def transcribe_audio(self, audio_path: Path) -> dict:
        try:
            chunks = chunk_audio(audio_path)
        except Exception as e:
            logger.error(f"Error chunking audio file {audio_path}: {e}")
            return None

        transcriptions = []

        for chunk in chunks:
            try:
                logger.info(f"Transcribing chunk: {chunk}")
                transcription = await self.transcribe_chunk(chunk)
                transcriptions.append(transcription)
            except Exception as e:
                logger.error(f"Failed to transcribe chunk {chunk}: {e}")

        # Combine transcriptions if there were multiple chunks
        if len(transcriptions) > 1:
            combined_transcription = self.combine_transcriptions(transcriptions)
        else:
            combined_transcription = transcriptions[0] if transcriptions else None

        return combined_transcription

    def combine_transcriptions(self, transcriptions: List[dict]) -> dict:
        combined = transcriptions[0].copy()
        for t in transcriptions[1:]:
            if 'results' in t and 'channels' in t['results']:
                combined['results']['channels'][0]['alternatives'][0]['transcript'] += ' ' + t['results']['channels'][0]['alternatives'][0]['transcript']
        return combined


    def fix_encoding(self, text):
        # Fix common encoding issues
        fixes = {
            'Ã£': 'ã', 'Ãµ': 'õ', 'Ã¡': 'á', 'Ã¢': 'â', 'Ã©': 'é',
            'Ãª': 'ê', 'Ã­': 'í', 'Ã³': 'ó', 'Ã´': 'ô', 'Ãº': 'ú',
            'Ã§': 'ç', 'Ã ': 'Á', 'Ã‰': 'É', 'Ã ': 'Í', 'Ã"': 'Ó',
            'Ãš': 'Ú', 'Ã€': 'À', 'Ãƒ': 'Ã'
        }
        for wrong, right in fixes.items():
            text = text.replace(wrong, right)
        return unicodedata.normalize('NFC', text)

    def json_to_markdown(self, json_data: dict) -> str:
        md_content = "# Transcription\n\n"
        
        # Add metadata
        md_content += "## Metadata\n\n"
        if 'metadata' in json_data:
            md_content += f"- Duration: {json_data['metadata'].get('duration', 'N/A')} seconds\n"
            md_content += f"- Channels: {json_data['metadata'].get('channels', 'N/A')}\n"
        md_content += f"- Model: {json_data.get('model', 'N/A')}\n\n"

        # Add transcript
        md_content += "## Transcript\n\n"
        if 'results' in json_data and 'channels' in json_data['results']:
            for result in json_data['results']['channels']:
                for alternative in result.get('alternatives', []):
                    transcript = alternative.get('transcript', '')
                    md_content += self.fix_encoding(transcript) + "\n\n"

        return md_content

    async def process_file(self, file_path: Path) -> None:
        async with self.semaphore:
            try:
                file_path = file_path.resolve()  # Ensure we're working with an absolute path
                transcript_path = file_path.with_suffix('.md')

                if transcript_path.exists():
                    logger.info(f"Transcript already exists for {file_path}. Skipping.")
                    return

                logger.info(f"Processing file: {file_path}")
                
                # Extract audio if the file is a video
                if file_path.suffix.lower() in ('.mp4', '.mov', '.avi', '.mkv', '.webm'):
                    audio_path = extract_audio_from_video(file_path)
                else:
                    audio_path = file_path

                logger.info(f"Transcribing audio from {audio_path}...")
                json_response = await self.transcribe_audio(audio_path)
                
                if json_response:
                    markdown_content = self.json_to_markdown(json_response)

                    logger.info(f"Writing transcript to {transcript_path}")
                    async with aiofiles.open(transcript_path, 'w', encoding='utf-8') as f:
                        await f.write(markdown_content)

                    logger.info(f"Transcript saved as {transcript_path}")
                else:
                    logger.error(f"Failed to obtain transcription for {audio_path}")

            except Exception as e:
                logger.exception(f"Error processing file {file_path}: {e}")
            finally:
                logger.info("Cleaning up temporary files...")
                self.cleanup_temp_files(file_path)

    def cleanup_temp_files(self, original_file_path: Path):
        directory = original_file_path.parent
        stem = original_file_path.stem
        for item in directory.glob(f"{stem}_chunk_*.wav"):
            try:
                item.unlink()
                logger.info(f"Deleted temporary file: {item}")
            except PermissionError:
                logger.warning(f"Unable to delete temporary file: {item}. It may be in use.")

    def get_media_files(self, directory: Path) -> List[Path]:
        if self.config.include_subfolders:
            return [
                file for file in directory.rglob('*')
                if file.is_file() and file.suffix.lower() in ('.mp3', '.wav', '.aac', '.m4a', '.flac', '.mp4', '.mov', '.avi', '.mkv', '.webm')
            ]
        else:
            return [
                file for file in directory.iterdir()
                if file.is_file() and file.suffix.lower() in ('.mp3', '.wav', '.aac', '.m4a', '.flac', '.mp4', '.mov', '.avi', '.mkv', '.webm')
            ]

    async def transcribe_all_in_directory(self, directory: Path) -> None:
        files = self.get_media_files(directory)
        tasks = [self.process_file(file) for file in files]
        await asyncio.gather(*tasks)

    def merge_transcriptions(self, directory: Path) -> None:
        transcripts = []
        for transcript_file in directory.glob('*.md'):
            with open(transcript_file, 'r', encoding='utf-8') as f:
                transcripts.append(f.read())

        merged_file = directory / 'merged_transcriptions.md'
        with open(merged_file, 'w', encoding='utf-8') as f:
            f.write('\n\n---\n\n'.join(transcripts))

        logger.info(f"Merged transcriptions saved as {merged_file}")