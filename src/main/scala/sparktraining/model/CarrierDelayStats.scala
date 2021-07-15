package sparktraining.model

case class CarrierDelayStats(carrier: String,
                             minDelay: Double,
                             maxDelay: Double,
                             avgDelay: Double
                             /*,
                             medianDelay: Double
                             */
                            )

