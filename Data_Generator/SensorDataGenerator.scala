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

