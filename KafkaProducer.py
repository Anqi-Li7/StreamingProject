# Databricks notebook source
import websocket
import json
import time
import threading
from json import dumps
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

# COMMAND ----------

brokers = 'b-1.detrainingmsk.66lq6h.c10.kafka.us-east-1.amazonaws.com:9092'
admin_client = KafkaAdminClient(bootstrap_servers=brokers, request_timeout_ms =30000)
topic_name = 'coin_cap_data2'
num_partitions = 3
replication_factor = 2
topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
admin_client.create_topics([topic])

producer = KafkaProducer(bootstrap_servers=brokers,api_version=(2,8,1))

# COMMAND ----------

def on_open(ws):
    print('Connection opened')
    #subscribe to the Bitcoin/USD trading pair
    ws.send(json.dumps({
        'type':'subscribe',
        'exchange':'Coinbase',
        'market':'BTC/USD',
        'channel':'trades'
    }).encode('utf-8'))
    print('Connection initialized')

def on_message(ws, message):
    data = json.loads(message)
    print(type(message))
    print(message)
    
    base = data['base']
    if base == 'bitcoin' or base == 'ethereum':
        producer.send(topic, value = message.encode('utf-8'))

def on_error(ws, error):
    print('Error:{}'.format(error))

def on_close(ws):
    print('Websocket closed')

    

# COMMAND ----------

trade_wss = 'wss://ws.coincap.io/trades/binance'

ws = websocket.WebSocketApp(
    trade_wss,
    on_open = on_open,
    on_message = on_message,
    on_close = on_close,
    on_error = on_error
)

ws.run_forever()

# COMMAND ----------

ws.close()
