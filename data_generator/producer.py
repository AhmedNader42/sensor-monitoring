from confluent_kafka import Producer
import numpy as np
import datetime
import uuid
from scipy.interpolate import make_interp_spline
from yielding import generator
import matplotlib.pyplot as plt
import matplotlib.animation as animation

import avro.schema
import io
from avro.io import DatumReader, DatumWriter, BinaryEncoder, BinaryDecoder


schema = avro.schema.parse(open("msg.avsc", "rb").read())

p = Producer({"bootstrap.servers": "localhost:9092"})

fig, ax = plt.subplots()
IDENTIFIER = "DEVICE1"


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""

    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


def generate_record(current_speed):
    generated_event_id = uuid.uuid4()
    generated_value_1 = next(generator(current_speed=current_speed))
    top_speed = generated_value_1[-1]

    print(type(generated_value_1))
    # WRITER
    writer = DatumWriter(schema)

    bytes_writer = io.BytesIO()

    encoder = BinaryEncoder(bytes_writer)

    writer.write(
        {
            "id": IDENTIFIER,
            "event_id": str(generated_event_id),
            "data_values": generated_value_1.tolist(),
            "event_timestamp": str(datetime.datetime.now()),
        },
        encoder,
    )
    message_bytes = bytes_writer.getvalue()

    return (
        message_bytes,
        top_speed,
        generated_value_1,
    )


simulated = np.array([0, 0, 0, 0, 0])
top_speed = 0


def produce_data():
    global simulated
    global top_speed

    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message. The delivery report callback will
    # be triggered from the call to poll() above, or flush() below, when the
    # message has been successfully delivered or failed permanently.
    message, top_speed, generated_value_1 = generate_record(top_speed)

    simulated = np.append(simulated, generated_value_1)

    p.produce(
        "line_data",
        key=IDENTIFIER,
        value=message,
        callback=delivery_report,
    )


x = np.arange(1, 6)
(line,) = ax.plot(x, simulated)


def animate(i):
    produce_data()

    if len(simulated) < 3:
        return

    x = np.arange(1, len(simulated) + 1)
    X_Y_Spline = make_interp_spline(x, simulated)
    # print(x)
    print(simulated)
    # Returns evenly spaced numbers
    # over a specified interval.
    X_ = np.linspace(x.min(), x.max(), 100)
    Y_ = X_Y_Spline(X_)
    ax.clear()

    ax.plot(X_, Y_)


ani = animation.FuncAnimation(fig, animate, interval=5000, frames=10)


plt.show()

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()

print("EXECUTION FINISHED")
# Save the final plot as a png
# plt.savefig("plot.png")
