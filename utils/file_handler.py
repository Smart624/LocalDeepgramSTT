from pathlib import Path
import os

def get_language_choice() -> str:
    LANGUAGE_CODES = {
        'e': 'en',
        'p': 'pt-BR',
        's': 'es',
        'a': 'auto'
    }

    while True:
        print("\nSelect language:")
        print("e - English")
        print("p - Portuguese (Brazilian)")
        print("s - Spanish")
        print("a - Auto-detect")
        choice = input("Enter your choice (e/p/s/a): ").strip().lower()
        if choice in LANGUAGE_CODES:
            return LANGUAGE_CODES[choice]
        print("Invalid choice. Please try again.")

def get_video_directory():
    while True:
        directory = input("Enter the full path to the directory containing your videos: ").strip()
        if os.path.isdir(directory):
            return Path(directory)
        print("Invalid directory. Please try again.")

def list_video_files(directory):
    SUPPORTED_AUDIO_FORMATS = ('.mp3', '.wav', '.aac', '.m4a', '.flac')
    SUPPORTED_VIDEO_FORMATS = ('.mp4', '.mov', '.avi', '.mkv', '.webm')
    video_files = [f for f in os.listdir(directory) if f.lower().endswith(SUPPORTED_AUDIO_FORMATS + SUPPORTED_VIDEO_FORMATS)]
    for i, file in enumerate(video_files, 1):
        print(f"{i}. {file}")
    return video_files

def select_video_file(video_files):
    while True:
        try:
            choice = int(input("Enter the number of the file you want to transcribe: "))
            if 1 <= choice <= len(video_files):
                return video_files[choice - 1]
        except ValueError:
            pass
        print("Invalid choice. Please try again.")
