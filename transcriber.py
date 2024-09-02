import os
import asyncio
import logging
from pathlib import Path
from typing import List, Dict, Tuple
from deepgram import DeepgramClient, PrerecordedOptions
import aiofiles
import backoff
from utils.audio_utils import extract_audio_from_video, chunk_audio, cleanup_audio_files
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from aiolimiter import AsyncLimiter

logger = logging.getLogger(__name__)

class AudioTranscriber:
    def __init__(self, config):
        self.config = config
        self.semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent requests for Pay-as-you-go plan
        self.deepgram = DeepgramClient(os.getenv('DG_API_KEY'))
        self.executor = ThreadPoolExecutor(max_workers=os.cpu_count())
        self.rate_limiter = AsyncLimiter(5, 1)  # 5 requests per second
        self.timeout = 600  # 10 minutes timeout

    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    async def transcribe_chunk(self, chunk_path: Path) -> dict:
        async with self.semaphore:
            async with self.rate_limiter:
                try:
                    with open(chunk_path, "rb") as audio:
                        buffer_data = audio.read()
                        source = {"buffer": buffer_data, "mimetype": "audio/aac"}
                        options = PrerecordedOptions(
                            model="nova-2",
                            smart_format=True,
                            language=self.config.language,
                            diarize=True,
                        )
                        
                        response = await asyncio.wait_for(
                            self.run_transcribe(source, options),
                            timeout=self.timeout
                        )
                        return response
                except asyncio.TimeoutError:
                    logger.error(f"Transcription for chunk {chunk_path} timed out after {self.timeout} seconds")
                    raise
                except Exception as e:
                    logger.error(f"Failed to transcribe chunk {chunk_path}: {str(e)}")
                    raise

    async def run_transcribe(self, source, options):
        return await asyncio.to_thread(
            self.deepgram.listen.prerecorded.v("1").transcribe_file,
            source,
            options
        )

    async def transcribe_audio(self, audio_path: Path) -> dict:
        try:
            chunks = await asyncio.get_event_loop().run_in_executor(
                self.executor, chunk_audio, audio_path
            )
        except Exception as e:
            logger.error(f"Error chunking audio file {audio_path}: {str(e)}")
            return None

        if not chunks:
            logger.error(f"No valid chunks created for {audio_path}")
            return None

        transcription_tasks = [self.transcribe_chunk(chunk) for chunk in chunks]
        transcriptions = await asyncio.gather(*transcription_tasks, return_exceptions=True)

        valid_transcriptions = []
        for i, result in enumerate(transcriptions):
            if isinstance(result, Exception):
                logger.error(f"Failed to transcribe chunk {i}: {str(result)}")
            else:
                valid_transcriptions.append(result.to_dict())

        await asyncio.get_event_loop().run_in_executor(self.executor, cleanup_audio_files, chunks)

        if len(valid_transcriptions) > 1:
            combined_transcription = self.combine_transcriptions(valid_transcriptions)
        elif len(valid_transcriptions) == 1:
            combined_transcription = valid_transcriptions[0]
        else:
            logger.error(f"No valid transcriptions for {audio_path}")
            return None

        return combined_transcription

    def combine_transcriptions(self, transcriptions: List[dict]) -> dict:
        combined = transcriptions[0].copy()
        for t in transcriptions[1:]:
            if 'results' in t and 'channels' in t['results']:
                combined['results']['channels'][0]['alternatives'][0]['transcript'] += ' ' + t['results']['channels'][0]['alternatives'][0]['transcript']
                if 'words' in combined['results']['channels'][0]['alternatives'][0]:
                    combined['results']['channels'][0]['alternatives'][0]['words'].extend(t['results']['channels'][0]['alternatives'][0].get('words', []))
                if 'paragraphs' in combined['results']['channels'][0]['alternatives'][0]:
                    combined['results']['channels'][0]['alternatives'][0]['paragraphs']['paragraphs'].extend(
                        t['results']['channels'][0]['alternatives'][0].get('paragraphs', {}).get('paragraphs', [])
                    )
        return combined

    def fix_encoding(self, text):
        fixes = {
            'Ã£': 'ã', 'Ãµ': 'õ', 'Ã¡': 'á', 'Ã¢': 'â', 'Ã©': 'é',
            'Ãª': 'ê', 'Ã­': 'í', 'Ã³': 'ó', 'Ã´': 'ô', 'Ãº': 'ú',
            'Ã§': 'ç', 'Ã ': 'Á', 'Ã‰': 'É', 'Ã ': 'Í', 'Ã"': 'Ó',
            'Ãš': 'Ú', 'Ã€': 'À', 'Ãƒ': 'Ã'
        }
        for wrong, right in fixes.items():
            text = text.replace(wrong, right)
        return unicodedata.normalize('NFC', text)

    async def json_to_markdown(self, json_data: dict, non_diarized_file: Path, diarized_file: Path) -> None:
        async with aiofiles.open(non_diarized_file, 'w', encoding='utf-8') as non_diarized_f, \
                   aiofiles.open(diarized_file, 'w', encoding='utf-8') as diarized_f:
            if 'results' in json_data and 'channels' in json_data['results']:
                for result in json_data['results']['channels']:
                    for alternative in result.get('alternatives', []):
                        transcript = alternative.get('transcript', '')
                        await non_diarized_f.write(self.fix_encoding(transcript) + "\n\n")
                        
                        if 'paragraphs' in alternative:
                            for paragraph in alternative['paragraphs'].get('paragraphs', []):
                                speaker = paragraph.get('speaker', 'Unknown')
                                paragraph_text = ""

                                for sentence in paragraph.get('sentences', []):
                                    text = sentence.get('text', '')
                                    paragraph_text += text + " "

                                if paragraph_text.strip():
                                    await diarized_f.write(f"Speaker {speaker}: {self.fix_encoding(paragraph_text.strip())}\n\n")

    async def process_file(self, file_path: Path) -> None:
        try:
            file_path = file_path.resolve()
            transcript_path = file_path.with_suffix('.md')
            diarized_transcript_path = file_path.with_name(f"{file_path.stem}_diarized.md")

            if transcript_path.exists() and diarized_transcript_path.exists():
                logger.info(f"Transcripts already exist for {file_path}. Skipping.")
                return

            logger.info(f"Processing file: {file_path}")
            
            if file_path.suffix.lower() in ('.mp4', '.mov', '.avi', '.mkv', '.webm'):
                audio_path = await asyncio.get_event_loop().run_in_executor(
                    self.executor, extract_audio_from_video, file_path
                )
            else:
                audio_path = file_path

            logger.info(f"Transcribing audio from {audio_path}...")
            json_response = await self.transcribe_audio(audio_path)
            
            if json_response:
                logger.info(f"Writing transcripts to {transcript_path} and {diarized_transcript_path}")
                await self.json_to_markdown(json_response, transcript_path, diarized_transcript_path)
                logger.info(f"Transcripts saved successfully")
            else:
                logger.error(f"Failed to obtain transcription for {audio_path}")
                
            if audio_path != file_path:
                await asyncio.get_event_loop().run_in_executor(
                    self.executor, cleanup_audio_files, [audio_path]
                )

        except Exception as e:
            logger.exception(f"Error processing file {file_path}: {str(e)}")

    async def transcribe_all_in_directory(self, directory: Path) -> None:
        files = self.get_media_files(directory)
        tasks = [self.process_file(file) for file in files]
        await asyncio.gather(*tasks)

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

    def merge_transcriptions(self, directory: Path) -> None:
        transcripts = []
        for transcript_file in directory.glob('*.md'):
            with open(transcript_file, 'r', encoding='utf-8') as f:
                transcripts.append(f.read())

        merged_file = directory / 'merged_transcriptions.md'
        with open(merged_file, 'w', encoding='utf-8') as f:
            f.write('\n\n---\n\n'.join(transcripts))

        logger.info(f"Merged transcriptions saved as {merged_file}")