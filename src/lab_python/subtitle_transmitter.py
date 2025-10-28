import json
from confluent_kafka import Producer
from typing import Literal, TypedDict

StartMessage = TypedDict(
    "StartMessage", {"cmd": Literal["start"], "channel": str, "program_id": str}
)


class SubtitleTransmitter:
    def __init__(self) -> None:
        self.producer = Producer(
            {
                "bootstrap.servers": "localhost:9092",
                "client.id": "python-producer",
            }
        )

    def start(self, channel: str, program_id: str) -> None:
        start_message: StartMessage = {
            "cmd": "start",
            "channel": channel,
            "program_id": program_id,
        }

        message_bytes = json.dumps(start_message).encode("utf-8")

        self.producer.produce(
            topic="subtitle_transmitter_actions",
            value=message_bytes,
            callback=lambda err, msg: print(f"err: {err} msg: {msg}"),
        )

        self.producer.flush()

    def stop(self) -> None:
        raise NotImplementedError
