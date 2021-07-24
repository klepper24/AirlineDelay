package sparktraining

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import sparktraining.model.{Flight, CarrierDelayStats}

object FlightsAggs {

    def getDelayMedian = (delayList: Seq[Double]) => {
      val count = delayList.size
      val sortedDelayList = delayList.sortWith(_ < _)
      if (count % 2 == 0) {
        val l = count / 2 - 1
        val r = l + 1
        (sortedDelayList(l) + sortedDelayList(r)) / 2
      } else
        sortedDelayList(count / 2)
    }

    val calculateMedian =  udf(getDelayMedian)


    def topNCarriers(delays: Dataset[Flight])
                    (n: Int, ascending: Boolean)
                    (implicit spark: SparkSession): Dataset[CarrierDelayStats] = {
        import spark.implicits._

        delays
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
          .as[CarrierDelayStats]

//        delays.createOrReplaceTempView("table")
//        spark.sql("select " +
//          "carrier, " +
//          "min(delay) minDelay, " +
//          "max(delay) maxDelay, " +
//          "avg(delay) avgDelay " +
//          "from table " +
//          "group by carrier " +
//          s"order by avgDelay ${if (ascending) "ASC" else "DESC"} " +
//          s"limit $n")
//          .as[CarrierDelayStats]
    }

}
