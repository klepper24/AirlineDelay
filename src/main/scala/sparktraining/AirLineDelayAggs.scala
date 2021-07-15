package sparktraining

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import sparktraining.model.{AirLineDelay, CarrierDelayStats}

object AirLineDelayAggs {

    def topNCarriers(delays: Dataset[AirLineDelay])
                    (n: Int, ascending: Boolean)
                    (implicit spark: SparkSession): Dataset[CarrierDelayStats] = {
        import spark.implicits._

        delays
          .groupBy($"carrier")
          .agg(
              min($"delay").alias("minDelay"),
              max($"delay").alias("maxDelay"),
              avg($"delay").alias("avgDelay")
          )
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
