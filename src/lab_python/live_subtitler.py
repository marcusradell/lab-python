import json
from confluent_kafka import Consumer, Producer
import pyaudio


class LiveSubtitler:
    def __init__(self) -> None:
        self.pyAudio = pyaudio.PyAudio()

        self.consumer = Consumer(
            {
                "bootstrap.servers": "localhost:9092",
                "client.id": "python-producer",
                "group.id": "python-consumer-group",
            }
        )

        self.producer = Producer(
            {
                "bootstrap.servers": "localhost:9092",
                "client.id": "python-producer",
            }
        )

    def listen(self):
        self.consumer.subscribe(["subtitle_transmitter_actions"])

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
                    self._start()
                else:
                    print(
                        f"Consumed unhandled event -> topic: {message_topic}, value: {message_value.decode('utf-8')}"
                    )

        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()

    def _start(self) -> None:
        microphone_stream = self.pyAudio.open(
            format=pyaudio.paInt16,
            channels=1,
            rate=16000,
            input=True,
            frames_per_buffer=1024,
        )

        while True:
            value = microphone_stream.read(4 * 1024)

            self.producer.produce(
                "speech_to_text",
                value=value,
                callback=lambda err, msg: print(f"err: {err} msg: {msg}"),
            )

            # Poll for delivery reports
            self.producer.poll(0)


live_subtitler = LiveSubtitler()
live_subtitler.listen()
