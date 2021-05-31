package sparktraining

import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import sparktraining.model.{AirLineDelay, CarrierDelayStats}

class AirLineDelayAggs {

  def topNCarriers(delays: Dataset[AirLineDelay])
                  (n: Int, ascending: Boolean)
                  (implicit sc: SparkContext): Dataset[CarrierDelayStats] = {
    ???
  }

}
