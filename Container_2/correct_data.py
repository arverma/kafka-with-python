from kafka import KafkaConsumer, KafkaProducer
import json
import time
from datetime import datetime
import pytz


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


def correct_data(data):
    incorrect_timestamp = data.get("myTimestamp")
    if incorrect_timestamp:
        my_timestamp = datetime.fromisoformat(incorrect_timestamp)
        data["myTimestamp"] = str(my_timestamp.astimezone(pytz.timezone("UTC")))
    return data


if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=['192.168.177.186:9092'],
        value_serializer=json_serializer
    )

    consumer = KafkaConsumer(
        "input_topic",
        bootstrap_servers='192.168.177.186:9092',
        auto_offset_reset='earliest'
    )

    print("START: Reading Incorrect data")
    for msg in consumer:
        data = json.loads(msg.value)
        print("RECEIVED<input_topic>\tIncorrect Record:\t{}".format(data))
        corrected_data = correct_data(data)
        print("SEND<output_topic>\tCorrect Record:\t{}".format(data))
        producer.send("output_topic", corrected_data)
        time.sleep(4)