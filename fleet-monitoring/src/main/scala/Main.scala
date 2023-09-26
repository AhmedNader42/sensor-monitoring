import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object Main {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local").appName("Monitoring").getOrCreate()

    val df = ss.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "line_data")
      .option("auto.offset.reset", "earliest")
      .option("inferSchema", "true")
      .load()

    df.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
      .awaitTermination()
  }
}