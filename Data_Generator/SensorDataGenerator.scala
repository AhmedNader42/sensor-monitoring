import scala.util.Random

val sensorId: () => Int = () => Random.nextInt(100000)
val data: () => Double = () => Random.nextDouble
val timestamp: () => Long = () => System.currentTimeMillis

val recordFunction: () => String = { () =>
    if (Random.nextDouble < 0.9) {
        Seq(sensorId().toString, timestamp(), data()).mkString(",")
    } else {
        "!!~corrupt~^&##$"
    }
}

val sensorDataGenerator = sc.parallelize(1 to 100).map(_ => recordFunction)
val sensorData = sensorDataGenerator.map(recordFun => recordFun())
sensorData.take(5)

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

val streamingContext = new StreamingContext(sc, Seconds(2))

import org.apache.spark.streaming.dstream.ConstantInputDStream
val rawDStream = new ConstantInputDStream(streamingContext, sensorData)

case class SensorData(sensorId: Int, timestamp: Long, value: Double)

import scala.util.Try
val schemaStream = rawDStream.flatMap { record =>
    val fields = record.split(",")
    Try {
        SensorData(fields(0).toInt, fields(1).toLong, fields(2).toDouble)
    }.toOption
}

import org.apache.spark.sql.SaveMode.Append
schemaStream.foreachRDD { rdd => 
    val df = rdd.toDF()
    val outputPath = "/tmp/iotstream"

    // def ts: String = 360.toString
    // Appending to Parquet files gets more expensive with time
    // That is why we need to split every time period in a production environment
    // df.write.mode(Append).format("parquet").save(s"${outputPath}-$ts.parquet")
    
    df.write.mode(Append).format("json").save(s"$outputPath.json")
}

streamingContext.start()
