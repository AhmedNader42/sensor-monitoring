from confluent_kafka import Producer
import numpy as np
import datetime
import uuid
import random
import time
from yielding import generator

p = Producer({"bootstrap.servers": "localhost:9092"})


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


def generate_record(current_speed):
    generated_record_id = uuid.uuid4()
    generated_value_1 = next(generator(current_speed=current_speed))
    generated_value_2 = random.randint(-10, 110)
    generated_record_timestamp = datetime.datetime.now()
    top_speed = generated_value_1[-1]
    return (
        str(generated_record_id)
        + ","
        + str(generated_value_1)
        + ","
        + str(generated_value_2)
        + ","
        + str(generated_record_timestamp),
        top_speed,
    )


top_speed = 0
for i in range(10):
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message. The delivery report callback will
    # be triggered from the call to poll() above, or flush() below, when the
    # message has been successfully delivered or failed permanently.
    message, top_speed = generate_record(top_speed)
    print(message)
    print(top_speed)

    p.produce("sensor_data", message.encode("utf-8"), callback=delivery_report)
    time.sleep(3.0)

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()
