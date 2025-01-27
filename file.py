import numpy as np
import uuid


class File:
    def __init__(self, file_hash: bytes, name: str, size: np.uint64, providers: list[uuid.UUID], file_path: str = None):
        self.hash = file_hash
        self.name = name
        self.size = size
        self.providers = providers
        self.file_path = file_hash

    def __str__(self):
        string = f"\t{self.name} {self.size}byte <hash:{self.hash.hex()}>\n"
        for provider in self.providers:
            string += f"\t\t{provider}\n"
        return string
