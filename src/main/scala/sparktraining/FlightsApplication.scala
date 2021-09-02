package sparktraining

import org.apache.spark.sql.{Dataset, SparkSession}
import sparktraining.dto.{Carrier, Flight}

object FlightsApplication extends App {
    val spark = SparkSession
      .builder()
      .appName("Flight")
      .getOrCreate()

    private implicit val sparkSession: SparkSession = spark

    val count = 10
    val flights: Dataset[Flight] = FlightsReader.read(args(0))
    val carriers: Dataset[Carrier] = CarriersReader.read(args(1))

    val results = FlightsAggs.topNCarriers(flights, carriers)(count, ascending = true)

    results.write.csv(args(2))
}
