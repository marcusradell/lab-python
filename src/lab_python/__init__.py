import questionary
from lab_python.subtitle_transmitter import SubtitleTransmitter

channel = "svt"
program_id = "1234567-001A"


def main() -> None:
    subtitle_transmitter = SubtitleTransmitter()

    print("Welcome to Subtitle Transmitter!")
    action: str = questionary.select(
        "Choose an action",
        choices=["Start", "Stop"],
    ).ask()  # returns value of selection

    if action == "Start":
        return subtitle_transmitter.start(channel=channel, program_id=program_id)
    if action == "Stop":
        return subtitle_transmitter.stop()

    print(f"Command {action} is not supported (yet).")
