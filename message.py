from numpy import byte

class Message:
    bytecodes = {
    "init": 0,
    "disconnect": 1,
    "data": 10,
    "data_end": 11,
}

    def __init__(self, control_byte: byte, length: int, content: bytes, sender: str = None) -> None:
        self.control_byte: byte = byte(control_byte)
        self.length: int = length
        self.content: bytes = content
        self.sender: str = f"{sender[0]}:{sender[1]}" if sender else None
        

    def to_bytes(self) -> bytes:
        return (
            self.control_byte.tobytes()
            + self.length.to_bytes(length=4, byteorder="big", signed=False)
            + self.content
        )
    
    def __str__(self):
        return f"[Message <b:{int(self.control_byte)}> <len:{self.length}> <sdr:{self.sender}>]"
