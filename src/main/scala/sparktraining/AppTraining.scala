package sparktraining

import org.apache.spark.sql.{Dataset, SparkSession}
import sparktraining.model.Flight

object AppTraining extends App {
    val spark = SparkSession
      .builder()
      .appName("Flight")
      .getOrCreate()

    private implicit val sparkSession: SparkSession = spark

    val count = 10
    val delays: Dataset[Flight] = FlightsReader.read(args(0))

    val results = FlightsAggs.topNCarriers(delays)(count, ascending = true)

    results.write.csv(args(1))
}
