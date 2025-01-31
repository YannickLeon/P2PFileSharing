import numpy as np
import uuid
import time
import hashlib
import os.path
from threading import Thread

class File:
    def __init__(self, file_hash: bytes, name: str, size: np.uint64, providers: list[uuid.UUID], file_path: str = None):
        self.hash = file_hash
        self.name = name
        self.size = size
        self.providers = providers
        self.file_path = file_path

    def __str__(self):
        string = f"\t{self.name} {self.size}byte <hash:{self.hash.hex()}>\n"
        for provider in self.providers:
            string += f"\t\t{provider}\n"
        return string
    
    # 1: file not found, 2: file changed
    def check_integrity(self):
        if not os.path.exists(self.file_path):
            print(f"[i] {self.name} was moved or deleted!")
            return 1
        file_hash = hashlib.sha1()
        with open(self.file_path, "rb") as f:
            while True:
                segment = f.read(file_hash.block_size)
                if not segment:
                    break
                file_hash.update(segment)
        file_hash = file_hash.digest()
        if file_hash != self.hash:
            print(f"[i] {self.name} was changed!")
            return 2
        return 0
