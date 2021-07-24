package sparktraining

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}
import sparktraining.model.Flight

object FlightsReader {

    def read(path: String)(implicit spark: SparkSession): Dataset[Flight] = {
        import spark.implicits._

        val customSchema = StructType(Array(
            StructField("OP_CARRIER", StringType, nullable = false),
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
          .select($"OP_CARRIER", $"ORIGIN", $"DEST", $"ARR_DELAY")
          .withColumnRenamed("OP_CARRIER", "carrier")
          .withColumnRenamed("ORIGIN", "sourceAirport")
          .withColumnRenamed("DEST", "destinationAirport")
          .withColumnRenamed("ARR_DELAY", "delay")
          .as[Flight]
    }

}
