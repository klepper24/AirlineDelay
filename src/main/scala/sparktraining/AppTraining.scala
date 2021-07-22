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

    AirLineDelayAggs.topNCarriers(delays)(count, ascending = true)
}
