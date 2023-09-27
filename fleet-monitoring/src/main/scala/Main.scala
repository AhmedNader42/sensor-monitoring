import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger

import java.nio.file.{Files, Paths}

object Main {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().master("local")
      //      .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1")
      .appName("Monitoring").getOrCreate()
    println("Attempt to load path**********************************")
    val schema = new String(Files.readAllBytes(Paths.get("/home/ahmed/Documents/github/monitoring/fleet-monitoring/msg.avsc")))
    println(schema)

    val df = ss.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "line_data")
      //      .option("auto.offset.reset", "earliest")
      .load()


    println("PRINTING SCHEMA !")
    df.printSchema()

    println("PRINTING AVRO SCHEMA !")
    val output = df
      .where(col("value").isNotNull)
      .select(from_avro(col("value"), schema).alias("Record_Data"))

    //    output.show(100, truncate = false)
//    output.printSchema()

    df.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
      .awaitTermination()

  }
}


// spark-shell --packages org.apache.spark:spark-avro_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1
