package sparktraining

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{Matchers, WordSpec}
import sparktraining.model.{Flight, CarrierDelayStats}

class FlightsAggsTest extends WordSpec with Matchers with DatasetSuiteBase {

    "FlightAggs" should {
        "calculate top N carriers" in {
            implicit val sparkSession: SparkSession = spark
            import spark.implicits._

            // given
            val path = this.getClass.getClassLoader.getResource("delays.csv").getPath
            val input: Dataset[Flight] = FlightsReader.read(path)

            // when
            val top_n: Dataset[CarrierDelayStats] = FlightsAggs.topNCarriers(input)(5, ascending = true)

            // then
            val ds = sc.parallelize(
                Seq(
                    CarrierDelayStats("EV", -180.0, -27.0, -80.0, -33.0),
                    CarrierDelayStats("US", -10.0, -10.0, -10.0, -10.0),
                    CarrierDelayStats("9E", -9.0, -9.0, -9.0, -9.0),
                    CarrierDelayStats("FL", -4.0, -4.0, -4.0, -4.0),
                    CarrierDelayStats("OO", -5.0, 0.0, -2.5, -2.5),
                )
            )

            val expectedDs = ds.toDF().as[CarrierDelayStats]

            assertDatasetEquals(top_n, expectedDs)
        }

        "calculate top N carriers when N is > count of carriers" in {
            implicit val sparkSession: SparkSession = spark
            import spark.implicits._

            // given
            val path = this.getClass.getClassLoader.getResource("delays.csv").getPath
            val input: Dataset[Flight] = FlightsReader.read(path)

            // when
            val top_n: Dataset[CarrierDelayStats] = FlightsAggs.topNCarriers(input)(10, ascending = true)

            // then
            val ds = sc.parallelize(
                Seq(
                    CarrierDelayStats("EV", -180.0, -27.0, -80.0, -33.0),
                    CarrierDelayStats("US", -10.0, -10.0, -10.0, -10.0),
                    CarrierDelayStats("9E", -9.0, -9.0, -9.0, -9.0),
                    CarrierDelayStats("FL", -4.0, -4.0, -4.0, -4.0),
                    CarrierDelayStats("OO", -5.0, 0.0, -2.5, -2.5),
                    CarrierDelayStats("YV", -2.0, -2.0, -2.0, -2.0),
                    CarrierDelayStats("XE", 17.0, 17.0, 17.0, 17.0),
                )
            )

            val expectedDs = ds.toDF().as[CarrierDelayStats]

            assertDatasetEquals(top_n, expectedDs)
        }

        "calculate the worse N carriers" in {
            implicit val sparkSession: SparkSession = spark
            import spark.implicits._

            // given
            val path = this.getClass.getClassLoader.getResource("delays.csv").getPath
            val input: Dataset[Flight] = FlightsReader.read(path)

            // when
            val top_n: Dataset[CarrierDelayStats] = FlightsAggs.topNCarriers(input)(2, ascending = false)

            // then
            val ds = sc.parallelize(
                Seq(
                    CarrierDelayStats("XE", 17.0, 17.0, 17.0, 17.0),
                    CarrierDelayStats("YV", -2.0, -2.0, -2.0, -2.0),
                )
            )

            val expectedDs = ds.toDF().as[CarrierDelayStats]

            assertDatasetEquals(top_n, expectedDs)
        }

    }
}
