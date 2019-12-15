
from kafka import KafkaConsumer
import logging
import json
import os
import smtplib
import ssl


def get_kafka_consumer(broker, topic):
    try:
        consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[broker],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    except Exception as e:
        print(e)
        exit()
    else:
        return consumer

print("GEMPI SELLERS")
broker = "localhost:9092"
topic = "seller"
data = list()
consumer = get_kafka_consumer(broker, topic) 
for message in consumer:
    data = message.value
    print("Halo : ", data['seller'])
    print("Ada Pesanan Dari : ",data['data'])
    

    
