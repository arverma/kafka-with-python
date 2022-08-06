from kafka import KafkaConsumer
import json
import time


if __name__ == "__main__":
    consumer = KafkaConsumer(
        "output_topic",
        bootstrap_servers='192.168.177.186:9092',
        auto_offset_reset='earliest'
    )

    print("START: Reading Incorrect data")
    for msg in consumer:
        data = json.loads(msg.value)
        print("RECEIVED<output_topic>\tCorrected Record:\t{}".format(data))
        time.sleep(4)
