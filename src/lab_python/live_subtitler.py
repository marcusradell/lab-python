import json
from confluent_kafka import Consumer


class LiveSubtitler:
    def __init__(self) -> None:
        self.consumer = Consumer(
            {
                "bootstrap.servers": "localhost:9092",
                "client.id": "python-producer",
                "group.id": "python-consumer-group",
            }
        )

        self.consumer.subscribe(["subtitle_transmitter_actions"])

    def listen(self):
        try:
            while True:
                message = self.consumer.poll(1.0)
                if message is None:
                    continue
                elif message.error():
                    print(f"ERROR: {format(message.error())}")
                    continue

                message_topic = message.topic()
                message_value = message.value()

                if message_value is None:
                    print(f"Consumed empty event from topic {message_topic}.")
                    continue

                message_payload = json.loads(message_value)
                if message_payload["cmd"] == "start":
                    print(
                        f"Consumed event -> topic: {message_topic}, channel: {message_payload['channel']}, program_id: {message_payload['program_id']}"
                    )
                else:
                    print(
                        f"Consumed unhandled event -> topic: {message_topic}, value: {message_value.decode('utf-8')}"
                    )

        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()


live_subtitler = LiveSubtitler()
live_subtitler.listen()
