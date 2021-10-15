from kafka import KafkaConsumer
import json
import config

def main():             
    consumer = KafkaConsumer(config.TOPIC_NAME)
    for msg in consumer:
        output = []
        output.append(json.loads(msg.value))
        print (json.loads(msg.value))
        print ('\n')

if __name__ == "__main__":
    main()