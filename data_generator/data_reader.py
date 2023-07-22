from pyspark.streaming import StreamingContext
from pyspark import SparkContext, Row
from pyspark.sql import SparkSession

# streamingContext = StreamingContext(sc, 4)

# sensor_data = streamingContext.textFileStream("/tmp/iotstream.json")


# def handler(rdd):
#     print(rdd.sample(False, 0.3).first())


# sensor_data.pprint()

# streamingContext.start()

spark_context = SparkContext("local[2]", "data")

spark_session = SparkSession(spark_context)

# Subscribe to topic test
df = (
    spark_session.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensor_data")
    .option("auto.offset.reset", "earliest")
    .load()
)


def handler(input_df, batch_id):
    print("BATCH ID: " + str(batch_id))

    def splitter(row):
        print("This is going to split each array element!")
        print(row)
        print(row.value.decode("utf-8"))
        print(type(row.value.decode("utf-8")))

        # .split(" ")
        return Row(
            key=row.key,
            value=row.value.decode("utf-8"),
            topic=row.topic,
            partition=row.partition,
            offset=row.offset,
            timestamp=row.timestamp,
            timestampType=row.timestampType,
        )

    # input_df.foreach(splitter)
    input_df.show()

    # inputs_after_split.write.format("kafka").option(
    #     "kafka.bootstrap.servers", "localhost:9092"
    # ).option("failOnDataLoss", "false").option("topic", "testingOutput").save()


query = (
    df.writeStream.format("console")
    .foreachBatch(handler)
    .trigger(processingTime="10 seconds")
    .start()
    .awaitTermination()
)
