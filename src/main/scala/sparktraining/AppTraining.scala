package sparktraining

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}
import sparktraining.model.AirLineDelay

object AppTraining extends App {
    val spark = SparkSession
      .builder()
      .appName("AirlineDelay")
      .getOrCreate()

    implicit val sc: SparkContext = spark.sparkContext

    val count = 10
    val delays: Dataset[AirLineDelay] = AirlineDelaysReader.read(args(0))

    AirLineDelayAggs.topNCarriers(delays)(count, ascending = true)
}
