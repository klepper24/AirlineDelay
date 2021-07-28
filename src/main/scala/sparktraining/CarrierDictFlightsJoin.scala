package sparktraining

import org.apache.spark.sql.{Dataset, SparkSession}
import sparktraining.model.{CarrierDelayStats, CarrierDict, CarrierFlight, CarrierNameDelayStats, Flight}

object CarrierDictFlightsJoin {

  def join(dict: Dataset[CarrierDict], delays: Dataset[Flight])
                  (implicit spark: SparkSession): Dataset[CarrierFlight] = {

    import spark.implicits._

    delays
      .joinWith(dict,
                delays("carrier") === dict("code"),
                "inner")
      .map{ case (del, di) => CarrierFlight(di.name, del.sourceAirport, del.destinationAirport, del.delay) }

  }

  def joinAgg(dict: Dataset[CarrierDict], delays: Dataset[CarrierDelayStats])
              (implicit spark: SparkSession): Dataset[CarrierNameDelayStats] = ???


}
