package sparktraining.model

case class CarrierNameDelayStats(carrierName: String,
                             minDelay: Double,
                             maxDelay: Double,
                             avgDelay: Double,
                             medianDelay: Double)

