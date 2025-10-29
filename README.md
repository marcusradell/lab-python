## Start-Up

`docker compose up` for Kafka.

`uv run src/lab_python/speech_to_text.py` to run the transcription based on the Kafka messages.

`uv run src/lab_python/live_subtitler.py` to start the microphone and produce a stream of Kafka messages with binary data.

The three services above are stopped with `ctrl+c`.

`uv run lab-python` to run the subtitle transmitter (sorry about the weird command name).
Choose the "Start" command once all services are running.
The service terminates once you choose a command.
