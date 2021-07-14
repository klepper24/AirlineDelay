package sparktraining.model

case class AirLineDelay(carrier: String,
                        sourceAirport: String,
                        destinationAirport: String,
                        delay: Double)
