package sparktraining

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import sparktraining.dto.{Carrier, Flight}
import sparktraining.model.CarrierDelayStats

object FlightsAggs {

    def topNCarriers(flights: Dataset[Flight],
                     carriers: Dataset[Carrier])
                    (n: Int, ascending: Boolean)
                    (implicit spark: SparkSession): Dataset[CarrierDelayStats] = {
        import spark.implicits._

        val calculateMedian =  udf(getDelayMedian)

        flights
          .groupBy($"carrier")
          .agg(
              min($"delay").alias("minDelay"),
              max($"delay").alias("maxDelay"),
              avg($"delay").alias("avgDelay"),
              collect_list($"delay").alias("carrierDelays"))
          .withColumn("medianDelay", calculateMedian($"carrierDelays"))
          .drop("carrierDelays")
          .orderBy(if (ascending) $"avgDelay".asc else $"avgDelay".desc)
          .limit(n)
          .withColumn("carrierName", lit("Unknown"))
          .as[CarrierDelayStats]
          .joinWith(carriers, flights("carrier") === carriers("code"), "left")
          .map {
              case (delayStats, null) => delayStats
              case (delayStats, carrier) => delayStats.copy(carrierName = carrier.name)
          }
    }

    private def getDelayMedian = (delayList: Seq[Double]) => {
        val count = delayList.size
        val sortedDelayList = delayList.sortWith(_ < _)
        if (count % 2 == 0) {
            val l = count / 2 - 1
            val r = l + 1
            (sortedDelayList(l) + sortedDelayList(r)) / 2
        } else
            sortedDelayList(count / 2)
    }

}
