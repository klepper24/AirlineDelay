package sparktraining

import org.apache.spark.sql.Dataset
import sparktraining.model.AirLineDelay

object AirlineDelaysReader {

    def read(path: String): Dataset[AirLineDelay] = ???

}
