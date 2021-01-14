#!/usr/bin/env python

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
import random
import time
import math

# Read arguments and configurations and initialize
args = ccloud_lib.parse_args()
config_file = args.config_file
conf = ccloud_lib.read_ccloud_config(config_file)

# Knobs and dials
number_of_customers = 100
max_credit_history_months = 36
credit_types = ['mortgage'] * 20 + ['personal_loan'] * 60 + ['credit_card'] * 20

# Create Producer instance
producer = Producer({
    'bootstrap.servers': conf['bootstrap.servers'],
    'sasl.mechanisms': conf['sasl.mechanisms'],
    'security.protocol': conf['security.protocol'],
    'sasl.username': conf['sasl.username'],
    'sasl.password': conf['sasl.password'],
})

# Delete Topics
ccloud_lib.delete_topic(conf,["credit_payment_history", "credit_utilization", "credit_applications"])

# Create Topics
ccloud_lib.create_topic(conf, "credit_payment_history")
ccloud_lib.create_topic(conf, "credit_utilization")
ccloud_lib.create_topic(conf, "credit_applications")

delivered_records = 0

def acked(err, msg):
    global delivered_records
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        print("Produced record to topic {} partition [{}] @ offset {}"
                .format(msg.topic(), msg.partition(), msg.offset()))

# Seed credit_payment_history
for customer_id in range(1, number_of_customers + 1 ):
    record_key = json.dumps({'customer_id': customer_id})
    paid_on_time_pct = random.randrange(60, 100)
    for payment_month in range(1, max_credit_history_months + 1):
        paid_on_time = random.randrange(100) < paid_on_time_pct
        record_value = json.dumps({'customer_id': customer_id, 'credit_type': random.choice(credit_types), 'paid_on_time': paid_on_time})
        producer.produce("credit_payment_history",key=str(customer_id), value=record_value, on_delivery=acked)
        producer.poll(0)

# Seed credit_utilization
for customer_id in range(1, number_of_customers + 1 ):
    total_limit = random.randrange(10000, 100000)
    total_balance = random.randrange(0, math.floor(total_limit/2))
    record_key = json.dumps({'customer_id': customer_id})
    record_value = json.dumps({'customer_id': customer_id, 'total_balance': total_balance, 'total_limit': total_limit})
    producer.produce("credit_utilization",  key=str(customer_id), value=record_value, on_delivery=acked)
    producer.poll(0)    

# Produce credit applications forever
while True:
    customer_id = random.randrange(1, number_of_customers + 1)
    record_key = json.dumps({'customer_id': customer_id}) 
    record_value = json.dumps({'customer_id': customer_id, 'credit_type': random.choice(credit_types), 'amount': random.randrange(1000, 100000)})
    producer.produce("credit_applications", key=str(customer_id), value=record_value, on_delivery=acked)
    producer.poll(0)        
    time.sleep(1)

producer.flush()
