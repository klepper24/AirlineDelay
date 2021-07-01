package sparktraining

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}
import sparktraining.model.AirLineDelay

object AirlineDelaysReader {

    def read(path: String)(implicit spark: SparkSession): Dataset[AirLineDelay] = {
        import spark.implicits._

        val customSchema = StructType(Array(
            StructField("ORIGIN", StringType, nullable = false),
            StructField("DEST", StringType, nullable = false),
            StructField("ARR_DELAY", DoubleType, nullable = false))
        )

        spark
          .read
          .option("header", "true")
          .option("delimiter",";")
          .schema(customSchema)
          .csv(path)
          .withColumnRenamed("ORIGIN", "sourceAirport")
          .withColumnRenamed("DEST", "destinationAirport")
          .withColumnRenamed("ARR_DELAY", "delay")
          .as[AirLineDelay]
    }

}
