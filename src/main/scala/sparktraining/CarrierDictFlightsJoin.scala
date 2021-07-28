package sparktraining

import org.apache.spark.sql.{Dataset, SparkSession}
import sparktraining.model.{CarrierDelayStats, CarrierDict, CarrierFlight, CarrierNameDelayStats, Flight}

object CarrierDictFlightsJoin {

  def join(dict: Dataset[CarrierDict], delays: Dataset[Flight])
                  (implicit spark: SparkSession): Dataset[CarrierFlight] = ???

  def joinAgg(dict: Dataset[CarrierDict], delays: Dataset[CarrierDelayStats])
              (implicit spark: SparkSession): Dataset[CarrierNameDelayStats] = ???


}
