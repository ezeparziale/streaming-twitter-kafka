from confluent_kafka.admin import AdminClient, NewTopic
import config

n_repicas = 1
n_partitions = 3

admin_client = AdminClient({
    "bootstrap.servers": config.SERVER_KAFKA
})

topic_list = []
topic_list.append(NewTopic(config.TOPIC_NAME, n_partitions, n_repicas))
fs = admin_client.create_topics(topic_list)

for topic, f in fs.items():
    try:
        f.result()
        print("Topic {} creado".format(topic))
    except Exception as e:
        print("Error al crear el topic {}: {}".format(topic, e))