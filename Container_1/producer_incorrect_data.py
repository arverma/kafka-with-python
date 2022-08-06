from kafka import KafkaProducer
import json
import time

incorrect_records = [
    {"myKey": 1, "myTimestamp": "2022-03-01T09:11:04+01:00"},
    {"myKey": 2, "myTimestamp": "2022-03-01T09:12:08+01:00"},
    {"myKey": 3, "myTimestamp": "2022-03-01T09:13:12+01:00"},
    {"myKey": 4, "myTimestamp": ""},
    {"myKey": 5, "myTimestamp": "2022-03-01T09:14:05+01:00"}
]


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=['192.168.177.186:9092'], value_serializer=json_serializer)

if __name__ == "__main__":
    for row in incorrect_records:
        print("SEND<input_topic>\tIncorrect Data:\t{}".format(row))
        producer.send("input_topic", row)
        time.sleep(4)