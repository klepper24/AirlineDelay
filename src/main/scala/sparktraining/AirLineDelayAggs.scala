package sparktraining

import org.apache.spark.sql.{Dataset, SparkSession}
import sparktraining.model.{AirLineDelay, CarrierDelayStats}

object AirLineDelayAggs {

  def topNCarriers(delays: Dataset[AirLineDelay])
                  (n: Int, ascending: Boolean)
                  (implicit spark: SparkSession): Dataset[CarrierDelayStats] = {
    ???
  }

}
