# -*- coding: utf-8 -*-
"""
Created on Tue Jan  3 11:31:11 2023

@author: usman
"""

from kafka import KafkaConsumer
import json
def jsondeserializer(data):
    return json.loads(data)
consumer=KafkaConsumer(bootstrap_servers=['[::1]:9092'],value_deserializer=jsondeserializer
                       ,auto_offset_reset='earliest')
consumer.subscribe(['industry_data'])
count=0
for msg in consumer:
    print(msg.value)