from numpy import byte
import uuid


class Message:
    bytecodes = {
        "init": 0,
        "identify": 1,
        "disconnect": 2,
        "data": 10,
        "data_end": 11,
    }

    def __init__(self, control_byte: byte, unique_id: uuid.UUID, length: int = 0, content: bytes = b"", sender: tuple[str, int] = [None, None]):
        self.control_byte: byte = byte(control_byte)
        self.length: int = length
        self.content: bytes = content
        self.uuid: uuid.UUID = unique_id
        self.sender: tuple[str, int] = sender

    def to_bytes(self) -> bytes:
        return (
            self.control_byte.tobytes()
            + self.uuid.bytes
            + self.length.to_bytes(length=4, byteorder="big", signed=False)
            + self.content
        )

    def __str__(self):
        return f"[Message <b:{int(self.control_byte)}> <uuid:{self.uuid}> <len:{self.length}> <sdr:{self.sender[0]}:{self.sender[1]}>]"
