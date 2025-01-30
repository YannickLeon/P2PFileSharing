import uuid
import traceback
import numpy as np


class FilePart:
    def __init__(self, name: str, file_hash: bytes, size: np.uint64, data: bytes, sender: uuid.UUID):
        self.name = name
        self.hash = file_hash
        self.size = size
        self.data = data
        self.sender = sender
        self.progress = size/float(len(data))

    def append(self, data):
        self.data += data
        self.progress = (float(len(self.data))/self.size)*100

    def save(self):
        try:
            target = open("./test/" + self.name, "wb")
            target.write(self.data)
            print(f"[i] File {self.name} saved successfully.")
        except Exception as e:
            print("[!] Error while saving file:")
            traceback.print_exc()

    def __str__(self):
        return f"{self.name}: {self.progress:.2f}%"