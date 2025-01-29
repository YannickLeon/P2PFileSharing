from numpy import byte
import numpy as np
import uuid


class Message:
    bytecodes = {
        "init": 0,
        "identify": 1,
        "disconnect": 2,
        "heartbeat": 3,
        "register": 10,
        "deregister": 11,
        "election": 20,
        "alive": 21,
        "leader": 22,
    }

    # Maybe change sender to be a reference to the connection object and add an addr field for broadcasts.
    def __init__(self, control_byte: byte, sender_uuid: uuid.UUID, length: int = 0, content: bytes = b"", id: np.uint16 = np.uint16(0), connection=None):
        self.control_byte: byte = byte(control_byte)
        self.length: int = length
        self.sender_uuid: uuid.UUID = sender_uuid
        self.id: np.uint16 = id
        self.content: bytes = content
        self.vector_content: bytes = b""
        self.connection = connection

    def to_bytes(self) -> bytes:
        return (
            self.control_byte.tobytes()
            + self.sender_uuid.bytes
            + self.id.tobytes()
            + self.length.to_bytes(length=4, byteorder="big", signed=False)
            + self.content
        )

    def __str__(self):
        return f"[Message <b:{int(self.control_byte)}> <uuid:{self.sender_uuid}> <id:{self.id}> <len:{self.length}> <sdr:{self.connection.addr[0]}:{self.connection.addr[1]}>]"

    @staticmethod
    def vector_clock_to_bytes(vector_clock: dict) -> bytes:
        # Convert a vector clock dictionary into bytes.
        clock_bytes = b""
        for node_uuid, timestamp in vector_clock.items():
            clock_bytes += node_uuid.bytes + timestamp.to_bytes(4, byteorder="big")
        return clock_bytes

    @staticmethod
    def bytes_to_vector_clock(vector_content: bytes) -> dict:
        # Convert bytes back into a vector clock dictionary.
        if len(vector_content) % 20 != 0:
            raise ValueError("Invalid vector_content length; must be a multiple of 20.")

        clock = {}
        try:
            while vector_content:
                node_uuid = uuid.UUID(bytes=vector_content[:16])
                timestamp = int.from_bytes(vector_content[16:20], byteorder="big")
                clock[node_uuid] = timestamp
                vector_content = vector_content[20:]
                print(timestamp)
        except Exception as e:
            print("[!] Error decoding vector clock from bytes:", e)
        
        return clock