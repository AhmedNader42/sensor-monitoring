from confluent_kafka import Producer
import datetime
import uuid
import random
from normal_distribution import generate_normal_distribution_with_corruption

p = Producer({"bootstrap.servers": "localhost:9092"})


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


def generate_record():
    generated_record_id = uuid.uuid4()
    random.random()
    generated_value_1 = generate_normal_distribution_with_corruption()
    generated_value_2 = random.randint(-10, 110)
    generated_record_timestamp = datetime.datetime.now()
    return (
        str(generated_record_id)
        + ","
        + str(generated_value_1)
        + ","
        + str(generated_value_2)
        + ","
        + str(generated_record_timestamp)
    )


for i in range(10):
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message. The delivery report callback will
    # be triggered from the call to poll() above, or flush() below, when the
    # message has been successfully delivered or failed permanently.
    message = generate_record().encode("utf-8")
    p.produce("sensor_data", message, key="KEY1", callback=delivery_report)

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()
