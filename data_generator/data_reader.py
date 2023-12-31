from pyspark.streaming import StreamingContext
from pyspark import SparkContext, Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg


spark_context = SparkContext("local[2]", "data")

spark_session = SparkSession(spark_context)

# Subscribe to topic test
df = (
    spark_session.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "line_data")
    .option("auto.offset.reset", "earliest")
    .load()
)


# df_aggregated = df.select(col("value"))


def handler(input_df, batch_id):
    print("BATCH ID: " + str(batch_id))

    # input_df.show(truncate=False)
    print("VALUE AS STRING")
    input_array = input_df.selectExpr("CAST(value as string)")

    input_array.show(truncate=False)
    print("INPUT ARRAY")
    # print(input_array)
    # for each in input_array:
    #     print(each)
    #     print(each.value)

    # exploded_input.show(truncate=False)
    # inputs_after_split.write.format("kafka").option(
    #     "kafka.bootstrap.servers", "localhost:9092"
    # ).option("failOnDataLoss", "false").option("topic", "testingOutput").save()


query = (
    df.writeStream.outputMode("append")
    .format("console")
    .foreachBatch(handler)
    .trigger(processingTime="10 seconds")
    .start()
    .awaitTermination()
)
