package sparktraining

import org.apache.spark.sql.{Dataset, SparkSession}
import sparktraining.model.Carrier

object CarriersReader {

    def read(path: String)(implicit spark: SparkSession): Dataset[Carrier] = ???

}
