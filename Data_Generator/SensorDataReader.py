from pyspark.streaming import StreamingContext


streamingContext = StreamingContext(sc, 4)

sensor_data = streamingContext.textFileStream("/tmp/iotstream.json")


def handler(rdd):
    print(rdd.sample(False, 0.3).first())


sensor_data.pprint()

streamingContext.start()
