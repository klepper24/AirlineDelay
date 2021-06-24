package sparktraining.model

case class CarrierDelayStats(carrier: String,
                             minDelay: Long,
                             maxDelay: Long,
                             avgDelay: Long,
                             medianDelay: Long)
