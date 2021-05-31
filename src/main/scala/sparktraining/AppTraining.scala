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

    val df = spark.read.csv(args(0))

    val aggs = new AirLineDelayAggs()

    val count = 10
    val delays: Dataset[AirLineDelay] = ???

    aggs.topNCarriers(delays)(count, ascending = true)
}
