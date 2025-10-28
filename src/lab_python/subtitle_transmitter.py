import json
from confluent_kafka import Producer
from typing import Literal, TypedDict

StartMessage = TypedDict(
    "StartMessage", {"cmd": Literal["start"], "channel": str, "program_id": str}
)


class SubtitleTransmitter:
    def __init__(self) -> None:
        self.kafka_producer_configuration = {
            "bootstrap.servers": "localhost:9092",
            "client.id": "python-producer",
        }

        self.producer = Producer(self.kafka_producer_configuration)

    def start(self) -> None:
        start_message: StartMessage = {
            "cmd": "start",
            "channel": "svt",
            "program_id": "1234567-001A",
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
