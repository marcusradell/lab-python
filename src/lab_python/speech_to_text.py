from confluent_kafka import Consumer
from faster_whisper import WhisperModel  # type: ignore
import numpy as np


class SpeechToText:
    def __init__(self) -> None:
        self.model = WhisperModel("large-v3", device="cpu", compute_type="int8")

        self.consumer = Consumer(
            {
                "bootstrap.servers": "localhost:9092",
                "client.id": "python-producer",
                "group.id": "python-consumer-group",
            }
        )

    def listen(self) -> None:
        self.consumer.subscribe(["speech_to_text"])

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

                # Convert to numpy array for transcription
                audio_np = (
                    np.frombuffer(message_value, dtype=np.int16).astype(np.float32)
                    / 32768.0
                )

                segments, _info = self.model.transcribe(audio_np, language="sv")  # type: ignore

                print(f"segments: {list(segments)}")

        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()


speechToText = SpeechToText()

speechToText.listen()
