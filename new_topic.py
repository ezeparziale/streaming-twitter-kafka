from confluent_kafka.admin import AdminClient, NewTopic

from config import settings

n_repicas = 1
n_partitions = 3

admin_client = AdminClient({"bootstrap.servers": settings.SERVER_KAFKA})

topic_list = []
topic_list.append(NewTopic(settings.TOPIC_NAME, n_partitions, n_repicas))
fs = admin_client.create_topics(topic_list)

for topic, f in fs.items():
    try:
        f.result()
        print("Topic {} created".format(topic))
    except Exception as e:
        print("Error creating topic {}: {}".format(topic, e))
