import questionary
from lab_python.subtitle_transmitter import SubtitleTransmitter


def main() -> None:
    subtitle_transmitter = SubtitleTransmitter()

    print("Welcome to Subtitle Transmitter!")
    action: str = questionary.select(
        "Choose an action",
        choices=["Start", "Stop"],
    ).ask()  # returns value of selection

    if action == "Start":
        return subtitle_transmitter.start()
    if action == "Stop":
        return subtitle_transmitter.stop()

    print(f"Command {action} is not supported (yet).")
