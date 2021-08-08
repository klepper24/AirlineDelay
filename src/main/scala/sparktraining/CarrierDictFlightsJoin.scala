package sparktraining

import org.apache.spark.sql.{Dataset, SparkSession}
import sparktraining.model.{CarrierDelayStats, CarrierDict, CarrierFlight, CarrierNameDelayStats, Flight}

object CarrierDictFlightsJoin {

  def joinAgg(dict: Dataset[CarrierDict], delays: Dataset[CarrierDelayStats])
              (implicit spark: SparkSession): Dataset[CarrierNameDelayStats] = {

    import spark.implicits._

    delays
      .joinWith(dict,
        delays("carrier") === dict("code"),
        "inner")
      .map{ case (del, di) => CarrierNameDelayStats(di.name, del.minDelay, del.maxDelay, del.avgDelay, del.medianDelay) }

  }


}
