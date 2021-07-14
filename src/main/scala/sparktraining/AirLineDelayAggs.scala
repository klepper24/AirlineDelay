package sparktraining

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, SparkSession}
import sparktraining.AppTraining.delays.sparkSession
import sparktraining.model.{AirLineDelay, CarrierDelayStats}

object AirLineDelayAggs {

  def topNCarriers(delays: Dataset[AirLineDelay])
                  (n: Int, ascending: Boolean)
                  (implicit spark: SparkSession): Dataset[CarrierDelayStats] = {

    delays.createOrReplaceTempView("table")
    if (ascending == false) {
      spark.sql("select carrier, min_by(carrier,delay) minDelay, max_by(carrier,delay) maxDelay, from table order by minDelay ASC limit n").as[CarrierDelayStats]
    }
    else {
      spark.sql("select carrier, min_by(carrier,delay) minDelay, max_by(carrier,delay) maxDelay, from table order by minDelay DESC limit n").toDF().as[CarrierDelayStats]
    }


  }

}
