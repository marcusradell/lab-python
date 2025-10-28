import json
import time
from confluent_kafka import Producer
from typing import TypedDict

Message = TypedDict("Message", {"type": str, "timestamp": float})


class SubtitleTransmitter:
    def __init__(self) -> None:
        self.kafka_producer_configuration = {
            "bootstrap.servers": "localhost:9092",
            "client.id": "python-producer",
        }

        self.producer = Producer(self.kafka_producer_configuration)

    def start(self) -> None:
        message: Message = {
            "type": "START",
            "timestamp": time.time(),
        }

        message_bytes = json.dumps(message).encode("utf-8")

        self.producer.produce(
            topic="subtitle_transmitter_actions",
            value=message_bytes,
            callback=lambda err, msg: print(f"err: {err} msg: {msg}"),
        )

        self.producer.flush()

    def stop(self) -> None:
        pass
