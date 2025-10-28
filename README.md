## Start-Up

`docker compose up` for Kafka.

`uv run src/lab_python/speech_to_text.py` to run the transcription based on the Kafka messages.
It will also write the raw bytes to a file.
Press ctrl+c once to start transcription of the entire file.

`uv run src/lab_python/live_subtitler.py` to start the microphone and produce a stream of Kafka messages with binary data.

`uv run lab-python` to run the subtitle transmitter (sorry about the weird command name).
Choose the "Start" command once all services are running.
