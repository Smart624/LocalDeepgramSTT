import time
import asyncio
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class FileHandler(FileSystemEventHandler):
    def __init__(self, transcriber):
        self.transcriber = transcriber

    def on_created(self, event):
        if not event.is_directory and event.src_path.lower().endswith(('.mp3', '.wav', '.aac', '.m4a', '.flac', '.mp4', '.mov', '.avi', '.mkv', '.webm')):
            asyncio.run(self.transcriber.process_file(Path(event.src_path)))

def watch_directory(directory: Path, transcriber):
    event_handler = FileHandler(transcriber)
    observer = Observer()
    observer.schedule(event_handler, str(directory), recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
