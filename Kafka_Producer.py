# -*- coding: utf-8 -*-
"""
Created on Tue Jan  3 10:23:02 2023

@author: usman
"""
from kafka import KafkaProducer
import csv
import json
def jsonserializer(data):
    return json.dumps(data).encode("utf-8")
    
producer=KafkaProducer(bootstrap_servers=['[::1]:9092'],value_serializer=jsonserializer)
with open('industry.csv',mode='r') as file:
    reader=csv.reader(file)
    for row in reader:
        producer.send('industry_data',row)
producer.send('industry_data',"end")
producer.flush()