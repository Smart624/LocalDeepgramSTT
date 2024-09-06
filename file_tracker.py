import json
from pathlib import Path
import hashlib
import time
import os
import logging
from datetime import datetime

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
                        return {"files": []}
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in {self.tracker_file}. Initializing with empty structure.")
                return {"files": []}
        return {"files": []}

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
        
        # Check if file hash exists in processed files
        if any(file['hash'] == file_hash for file in self.processed_files['files']):
            return True
        
        # Check for existence of .md files
        non_diarized_md = file_path.with_suffix('.md')
        diarized_md = file_path.with_name(f"{file_path.stem}_diarized.md")
        
        if non_diarized_md.exists() or diarized_md.exists():
            # If .md files exist but the file is not in our tracker, add it
            self.mark_file_as_processed(file_path)
            return True
        
        return False

    def mark_file_as_processed(self, file_path: Path):
        file_hash = self.get_file_hash(file_path)
        file_info = {
            "hash": file_hash,
            "name": file_path.name,
            "directory": str(file_path.parent),
            "size": os.path.getsize(file_path),
            "processed_time": time.time(),
            "path": str(file_path)
        }
        # Check if the file is already in the list
        if not any(file['hash'] == file_hash for file in self.processed_files['files']):
            self.processed_files['files'].append(file_info)
            self.save_processed_files()

    def remove_file(self, file_hash: str):
        self.processed_files['files'] = [file for file in self.processed_files['files'] if file['hash'] != file_hash]
        self.save_processed_files()
        return True

    def get_all_processed_files(self):
        return self.processed_files['files']

    def get_processed_files_in_directory(self, directory: Path):
        return [file for file in self.processed_files['files'] if Path(file['directory']) == directory]