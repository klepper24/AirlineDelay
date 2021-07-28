package sparktraining

import org.apache.spark.sql.{Dataset, SparkSession}
import sparktraining.model.{CarrierDict, CarrierFlight, Flight}

object CarrierDictFlightsJoin {

  def joinFunc(dict: Dataset[CarrierDict], delays: Dataset[Flight])
                  (implicit spark: SparkSession): Dataset[CarrierFlight] = ???


}
