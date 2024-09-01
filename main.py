import asyncio
import logging
import os
from pathlib import Path
from config import load_config, save_config
from transcriber import AudioTranscriber
from utils.file_handler import get_language_choice, get_video_directory, list_video_files, select_video_file
from watch import watch_directory

# Initialize logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def main() -> None:
    config = load_config()
    transcriber = AudioTranscriber(config)

    if not config.default_directory:
        config.default_directory = str(get_video_directory())
        save_config(config)

    video_directory = Path(config.default_directory)
    logger.info(f"Working directory: {video_directory}")
    
    # Ensure the directory exists before changing to it
    if not video_directory.exists():
        logger.error(f"Directory {video_directory} does not exist.")
        return
    
    os.chdir(video_directory)

    if config.watch_directory:
        print(f"Watching directory: {video_directory}")
        watch_directory(video_directory, transcriber)
    else:
        while True:
            print("\nAudio Transcription Tool")
            print("1. Transcribe all media files in the current directory")
            print("2. Transcribe a specific media file")
            print("3. Merge all transcriptions in the current directory")
            print("4. Change default directory")
            print("5. Toggle watch mode")
            print("6. Exit")

            choice = input("Enter your choice (1/2/3/4/5/6): ").strip()

            if choice == '6':
                print("Exiting the program. Goodbye!")
                break

            if choice in ['1', '2']:
                config.language = get_language_choice()

            if choice == '1':
                config.include_subfolders = input("Include subfolders? (y/n): ").strip().lower() == 'y'
                await transcriber.transcribe_all_in_directory(video_directory)
            elif choice == '2':
                video_files = list_video_files(video_directory)
                if video_files:
                    selected_file = select_video_file(video_files)
                    file_path = video_directory / selected_file
                    await transcriber.process_file(file_path)
                else:
                    print("No video files found in the specified directory.")
            elif choice == '3':
                transcriber.merge_transcriptions(video_directory)
            elif choice == '4':
                config.default_directory = str(get_video_directory())
                save_config(config)
                video_directory = Path(config.default_directory)
                os.chdir(video_directory)
            elif choice == '5':
                config.watch_directory = not config.watch_directory
                save_config(config)
                if config.watch_directory:
                    print(f"Watch mode enabled. Watching directory: {video_directory}")
                    watch_directory(video_directory, transcriber)
                else:
                    print("Watch mode disabled.")
            else:
                logger.error("Invalid choice. Please try again.")

if __name__ == "__main__":
    asyncio.run(main())
