package sparktraining

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}
import sparktraining.model.Carrier

object CarriersReader {

    def read(path: String)(implicit spark: SparkSession): Dataset[Carrier] = {
            import spark.implicits._

            val customSchema = StructType(Array(
                StructField("Code", StringType, nullable = false),
                StructField("Description", StringType, nullable = false))
            )

            spark
              .read
              .option("header", "true")
              .option("delimiter",",")
              .schema(customSchema)
              .csv(path)
              .withColumnRenamed("Code", "code")
              .withColumnRenamed("Description", "name")
              .as[Carrier]
        }

}
