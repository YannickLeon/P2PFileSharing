import uuid
import traceback


class FilePart:
    def __init__(self, name: str, file_hash: bytes, data: bytes, sender: uuid.UUID):
        self.data = data
        self.sender = sender
        self.name = name
        self.hash = file_hash

    def save(self):
        try:
            target = open("./test/" + self.name, "wb")
            target.write(self.data)
            print(f"[i] File {self.name} saved successfully.")
        except Exception as e:
            print("[!] Error while saving file:")
            traceback.print_exc()
