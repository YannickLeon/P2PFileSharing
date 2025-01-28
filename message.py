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
    def __init__(self, control_byte: byte, sender_uuid: uuid.UUID, length: int = 0, content: bytes = b"", id: np.uint16 = np.uint16(0), connection=None):
        self.control_byte: byte = byte(control_byte)
        self.length: int = length
        self.sender_uuid: uuid.UUID = sender_uuid
        self.id: np.uint16 = id
        self.content: bytes = content
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
