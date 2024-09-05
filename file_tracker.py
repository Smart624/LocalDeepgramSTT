import json
from pathlib import Path
import hashlib
import time
import os
import logging

logger = logging.getLogger(__name__)

class FileTracker:
    def __init__(self, script_dir: Path):
        self.script_dir = script_dir
        self.tracker_file = self.script_dir / 'processed_files.json'
        self.processed_files = self.load_processed_files()

    def load_processed_files(self):
        if self.tracker_file.exists():
            try:
                with open(self.tracker_file, 'r') as f:
                    content = f.read().strip()
                    if content:
                        return json.loads(content)
                    else:
                        logger.warning(f"The file {self.tracker_file} is empty. Initializing with empty structure.")
                        return {"directories": {}}
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in {self.tracker_file}. Initializing with empty structure.")
                return {"directories": {}}
        return {"directories": {}}

    def save_processed_files(self):
        with open(self.tracker_file, 'w') as f:
            json.dump(self.processed_files, f, indent=2)

    def get_file_hash(self, file_path: Path) -> str:
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def is_file_processed(self, file_path: Path) -> bool:
        file_hash = self.get_file_hash(file_path)
        dir_path = str(file_path.parent)
        return dir_path in self.processed_files["directories"] and file_hash in self.processed_files["directories"][dir_path]["files"]

    def mark_file_as_processed(self, file_path: Path):
        file_hash = self.get_file_hash(file_path)
        dir_path = str(file_path.parent)
        file_name = file_path.name
        file_size = os.path.getsize(file_path)
        current_time = time.time()

        if dir_path not in self.processed_files["directories"]:
            self.processed_files["directories"][dir_path] = {"files": {}}

        self.processed_files["directories"][dir_path]["files"][file_hash] = {
            "name": file_name,
            "size": file_size,
            "processed_time": current_time,
            "path": str(file_path)
        }
        self.save_processed_files()

    def remove_file(self, file_path: Path):
        file_hash = self.get_file_hash(file_path)
        dir_path = str(file_path.parent)
        if dir_path in self.processed_files["directories"]:
            if file_hash in self.processed_files["directories"][dir_path]["files"]:
                del self.processed_files["directories"][dir_path]["files"][file_hash]
                if not self.processed_files["directories"][dir_path]["files"]:
                    del self.processed_files["directories"][dir_path]
                self.save_processed_files()
                return True
        return False

    def get_processed_files_in_directory(self, directory: Path):
        dir_path = str(directory)
        return self.processed_files["directories"].get(dir_path, {}).get("files", {})

    def get_all_processed_files(self):
        return {dir_path: dir_data["files"] for dir_path, dir_data in self.processed_files["directories"].items()}