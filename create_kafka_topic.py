from kafka.admin import KafkaAdminClient, NewTopic

if __name__ == '__main__':
    admin_client = KafkaAdminClient(
        bootstrap_servers="192.168.177.186:9092"
    )

    topic_names = ["input_topic", "output_topic"]
    topic_list = []
    for t_name in topic_names:
        topic_list.append(NewTopic(name=t_name, num_partitions=1, replication_factor=1))
    print(admin_client.create_topics(new_topics=topic_list, validate_only=False))
    print("Topics created: {}".format(topic_names))
