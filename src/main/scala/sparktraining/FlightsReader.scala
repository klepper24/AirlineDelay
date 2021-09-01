package sparktraining

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}
import sparktraining.dto.Flight

object FlightsReader {

    def read(path: String)(implicit spark: SparkSession): Dataset[Flight] = {
        import spark.implicits._

        val customSchema = StructType(Array(
            StructField("FL_DATE", StringType, nullable = false),
            StructField("OP_CARRIER", StringType, nullable = false),
            StructField("OP_CARRIER_FL_NUM", StringType, nullable = false),
            StructField("ORIGIN", StringType, nullable = false),
            StructField("DEST", StringType, nullable = false),
            StructField("CRS_DEP_TIME", DoubleType, nullable = false),
            StructField("DEP_TIME", DoubleType, nullable = false),
            StructField("DEP_DELAY", DoubleType, nullable = false),
            StructField("TAXI_OUT", DoubleType, nullable = false),
            StructField("WHEELS_OFF", DoubleType, nullable = false),
            StructField("WHEELS_ON", DoubleType, nullable = false),
            StructField("TAXI_IN", DoubleType, nullable = false),
            StructField("CRS_ARR_TIME", DoubleType, nullable = false),
            StructField("ARR_TIME", DoubleType, nullable = false),
            StructField("ARR_DELAY", DoubleType, nullable = false),
            StructField("CANCELLED", DoubleType, nullable = false),
            StructField("CANCELLATION_CODE", DoubleType, nullable = false),
            StructField("DIVERTED", DoubleType, nullable = false),
            StructField("CRS_ELAPSED_TIME", DoubleType, nullable = false),
            StructField("ACTUAL_ELAPSED_TIME", DoubleType, nullable = false),
            StructField("AIR_TIME", DoubleType, nullable = false),
            StructField("DISTANCE", DoubleType, nullable = false),
            StructField("CARRIER_DELAY", DoubleType, nullable = false),
            StructField("WEATHER_DELAY", DoubleType, nullable = false),
            StructField("NAS_DELAY", DoubleType, nullable = false),
            StructField("SECURITY_DELAY", DoubleType, nullable = false),
            StructField("LATE_AIRCRAFT_DELAY", DoubleType, nullable = false))
        )



        spark
          .read
          .option("header", "true")
          .option("delimiter",",")
          .schema(customSchema)
          .csv(path)
          .select($"OP_CARRIER", $"ORIGIN", $"DEST", $"ARR_DELAY")
          .withColumnRenamed("OP_CARRIER", "carrier")
          .withColumnRenamed("ORIGIN", "sourceAirport")
          .withColumnRenamed("DEST", "destinationAirport")
          .withColumnRenamed("ARR_DELAY", "delay")
          .as[Flight]
    }

    def read_old(path: String)(implicit spark: SparkSession): Dataset[Flight] = {
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
