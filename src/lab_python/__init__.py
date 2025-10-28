import questionary


def main() -> None:
    print("Welcome to Subtitle Transmitter!")
    action: str = questionary.select(
        "Choose an action",
        choices=["Start", "Stop"],
    ).ask()  # returns value of selection

    if action == "Start":
        pass
    if action == "Stop":
        pass

    print(f"Command {action} is not supported (yet).")
