from confluent_kafka import Consumer


def liveSubtitler() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "client.id": "python-producer",
            "group.id": "python-consumer-group",
        }
    )

    consumer.subscribe(["subtitle_transmitter_actions"])

    # Poll for new messages from Kafka and print them.

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            elif message.error():
                print(f"ERROR: {format(message.error())}")
                continue

            message_topic = message.topic()
            message_value = message.value()

            if message_value is None:
                print(f"Consumed empty event from topic {message_topic}.")
            else:
                print(
                    f"Consumed event from topic {message_topic}, value = {message_value.decode('utf-8')}"
                )
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()


liveSubtitler()
