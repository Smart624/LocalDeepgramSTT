import asyncio
import logging
import os
from pathlib import Path
from config import load_config, save_config
from transcriber import AudioTranscriber
from utils.file_handler import get_language_choice, list_video_files, select_video_file
from watch import watch_directory
from file_tracker import FileTracker
from datetime import datetime

# Initialize logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_video_directory():
    while True:
        directory = input("Enter the full path to the directory containing your videos: ").strip()
        if os.path.isdir(directory):
            return Path(directory)
        print("Invalid directory. Please try again.")

def print_and_manage_processed_files(file_tracker, directory=None):
    while True:
        files = file_tracker.get_processed_files_in_directory(directory) if directory else file_tracker.get_all_processed_files()
        
        if not files:
            print("No processed files found.")
            return

        print("\nProcessed Files:")
        print("{:<5} {:<40} {:<20} {:<15} {:<25}".format("No.", "File Name", "Size", "Processed Date", "Directory"))
        print("-" * 105)

        for i, file in enumerate(files, 1):
            size = f"{file['size'] / 1024 / 1024:.2f} MB"
            processed_date = datetime.fromtimestamp(file['processed_time']).strftime('%Y-%m-%d %H:%M:%S')
            print("{:<5} {:<40} {:<20} {:<15} {:<25}".format(
                i, 
                file['name'][:37] + '...' if len(file['name']) > 40 else file['name'], 
                size, 
                processed_date, 
                Path(file['directory']).name
            ))

        print("\nOptions:")
        print("1. Remove a file from the processed list")
        print("2. View details of a specific file")
        print("3. Return to main menu")

        choice = input("Enter your choice (1-3): ").strip()

        if choice == '1':
            file_number = input("Enter the number of the file to remove: ")
            try:
                file_number = int(file_number)
                if 1 <= file_number <= len(files):
                    file_to_remove = files[file_number - 1]
                    if file_tracker.remove_file(file_to_remove['hash']):
                        print(f"File {file_to_remove['name']} removed from processed files.")
                    else:
                        print(f"Failed to remove {file_to_remove['name']} from processed files.")
                else:
                    print("Invalid file number.")
            except ValueError:
                print("Invalid input. Please enter a number.")

        elif choice == '2':
            file_number = input("Enter the number of the file to view details: ")
            try:
                file_number = int(file_number)
                if 1 <= file_number <= len(files):
                    file_details = files[file_number - 1]
                    print("\nFile Details:")
                    print(f"Name: {file_details['name']}")
                    print(f"Directory: {file_details['directory']}")
                    print(f"Size: {file_details['size'] / 1024 / 1024:.2f} MB")
                    print(f"Processed Time: {datetime.fromtimestamp(file_details['processed_time']).strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"File Hash: {file_details['hash']}")
                    input("Press Enter to continue...")
                else:
                    print("Invalid file number.")
            except ValueError:
                print("Invalid input. Please enter a number.")

        elif choice == '3':
            return

        else:
            print("Invalid choice. Please try again.")
async def merge_transcriptions(transcriber, directory: Path, include_subfolders: bool):
    def merge_files(dir_path, file_suffix):
        transcripts = []
        for transcript_file in dir_path.glob(f'*{file_suffix}.md'):
            with open(transcript_file, 'r', encoding='utf-8') as f:
                transcripts.append(f.read())
        
        if transcripts:
            merged_file = dir_path / f'merged{file_suffix}.md'
            with open(merged_file, 'w', encoding='utf-8') as f:
                f.write('\n\n---\n\n'.join(transcripts))
            print(f"Merged transcriptions saved as {merged_file}")

    merge_files(directory, '')  # Non-diarized
    merge_files(directory, '_diarized')  # Diarized

    if include_subfolders:
        for subdir in directory.iterdir():
            if subdir.is_dir():
                await merge_transcriptions(transcriber, subdir, include_subfolders)

async def main() -> None:
    script_dir = Path(__file__).parent.resolve()
    file_tracker = FileTracker(script_dir)
    config = load_config()
    transcriber = AudioTranscriber(config, file_tracker)

    if not config.default_directory:
        config.default_directory = str(get_video_directory())
        save_config(config)

    video_directory = Path(config.default_directory)
    logger.info(f"Working directory: {video_directory}")
    
    if not video_directory.exists():
        logger.error(f"Directory {video_directory} does not exist.")
        return
    
    os.chdir(video_directory)

    while True:
        print("\nAudio Transcription Tool")
        print("1. Transcribe all media files in the current directory")
        print("2. Transcribe a specific media file")
        print("3. Merge all transcriptions in the current directory")
        print("4. Change default directory")
        print("5. Toggle watch mode")
        print("6. List and manage processed files")
        print("7. Exit")

        choice = input("Enter your choice (1-7): ").strip()

        if choice == '7':
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
            include_subfolders = input("Include subfolders? (y/n): ").strip().lower() == 'y'
            await merge_transcriptions(transcriber, video_directory, include_subfolders)
        elif choice == '4':
            new_directory = get_video_directory()
            config.default_directory = str(new_directory)
            save_config(config)
            video_directory = new_directory
            os.chdir(video_directory)
            print(f"Default directory changed to: {video_directory}")
        elif choice == '5':
            config.watch_directory = not config.watch_directory
            save_config(config)
            if config.watch_directory:
                print(f"Watch mode enabled. The program will automatically process new files added to: {video_directory}")
                print("Press Ctrl+C to stop watching and return to the main menu.")
                try:
                    watch_directory(video_directory, transcriber)
                except KeyboardInterrupt:
                    print("\nWatch mode disabled. Returning to main menu.")
                    config.watch_directory = False
                    save_config(config)
            else:
                print("Watch mode disabled.")
        elif choice == '6':
            print_and_manage_processed_files(file_tracker, video_directory)
        else:
            logger.error("Invalid choice. Please try again.")

if __name__ == "__main__":
    asyncio.run(main())