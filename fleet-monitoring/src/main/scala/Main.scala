import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local")
      .appName("Monitoring").getOrCreate()


    val df = ss.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "line_data")
      .load()

    //    println("PRINTING AVRO SCHEMA !")
    //    val output = df
    //      .where(col("value").isNotNull)
    //      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    //    output.show(100, truncate = false)
    //    output.printSchema()


    df.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .foreachBatch(
        (outputDf: DataFrame, bid: Long) => {
          // Process valid data frames only
          if (!outputDf.isEmpty) {
            // business logic
            val newdf = outputDf
              .selectExpr("CAST(key as string)")
            newdf.printSchema()
          }
          //          df.select(from_avro(col("value"), schema)).show()
          //          df.selectExpr("CAST(key AS STRING)").show()
        }
      ).start()
      .awaitTermination()

  }

}


// spark-shell --packages org.apache.spark:spark-avro_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1
