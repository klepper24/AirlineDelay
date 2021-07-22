package sparktraining

import org.apache.spark.sql.{Dataset, SparkSession}
import sparktraining.model.AirLineDelay

object AppTraining extends App {
    val spark = SparkSession
      .builder()
      .appName("AirlineDelay")
      .getOrCreate()

    private implicit val sparkSession: SparkSession = spark

    val count = 10
    val delays: Dataset[AirLineDelay] = AirlineDelaysReader.read(args(0))

    val results = AirLineDelayAggs.topNCarriers(delays)(count, ascending = true)

    results.write.csv(args(1))
}
