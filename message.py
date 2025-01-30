from numpy import byte
import numpy as np
import uuid


class Message:
    bytecodes = {
        "init": 0,          # used for discovery
        "identify": 1,      # transmit uuid after connecting
        "disconnect": 2,    # disconnect peer with uuid
        "heartbeat": 3,
        "abort": 4,         # close duplicate socket
        "register": 10,     # register a file
        "deregister": 11,   # de-register a file
        "request": 12,      # request a file
        "data": 13,         # file data
        "dataend": 14,      # final data package
        "election": 20,     # trigger election
        "alive": 21,
        "leader": 22,       # announce leader by uuid
    }

    # Maybe change sender to be a reference to the connection object and add an addr field for broadcasts.
    def __init__(self, control_byte: byte, sender_uuid: uuid.UUID, length: int = 0, content: bytes = b"", id: np.uint16 = np.uint16(0), vector: bytes = b"", connection=None):
        self.control_byte: byte = byte(control_byte)
        self.length: int = length
        self.sender_uuid: uuid.UUID = sender_uuid
        self.id: np.uint16 = id
        self.content: bytes = content
        self.vector: bytes = vector
        self.vector_length: np.uint16 = np.uint16(len(vector))
        self.connection = connection

    def to_bytes(self) -> bytes:
        return (
            self.control_byte.tobytes()
            + self.vector_length.tobytes()
            + self.vector
            + self.sender_uuid.bytes
            + self.id.tobytes()
            + self.length.to_bytes(length=4, byteorder="big", signed=False)
            + self.content
        )

    def __str__(self):
        return f"[Message <b:{int(self.control_byte)}> <vectorlen:{self.vector_length}> <uuid:{self.sender_uuid}> <id:{self.id}> <len:{self.length}> <sdr:{self.connection.addr[0]}:{self.connection.addr[1]}>]"

    def set_vector(self, vector: bytes):
        self.vector = vector
        self.vector_length = np.uint16(len(vector))

    @staticmethod
    def vector_clock_to_bytes(vector_clock: dict[uuid.UUID, np.uint16]) -> bytes:
        # Convert a vector clock dictionary into bytes.
        clock_bytes = b""
        for node_uuid, timestamp in vector_clock.items():
            clock_bytes += node_uuid.bytes + timestamp.tobytes()
        return clock_bytes

    @staticmethod
    def vector_clock_from_bytes(vector_content: bytes) -> dict:
        # Convert bytes back into a vector clock dictionary.
        if len(vector_content) % 18 != 0:
            raise ValueError("Invalid vector_content length; must be a multiple of 18.")
        clock = {}
        try:
            while vector_content:
                node_uuid = uuid.UUID(bytes=vector_content[:16])
                timestamp = np.frombuffer(vector_content[16:18], dtype=np.uint16)[0]
                clock[node_uuid] = timestamp
                vector_content = vector_content[18:]
        except Exception as e:
            print("[!] Error decoding vector clock from bytes:", e)

        return clock
