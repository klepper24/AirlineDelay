package sparktraining

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{Matchers, WordSpec}
import sparktraining.model.{AirLineDelay, CarrierDelayStats}

class AirLineDelayAggsTest extends WordSpec with Matchers with DatasetSuiteBase {

    "AirLineDelayAggs" should {
        "calculate top N carriers" in {
            implicit val sparkSession: SparkSession = spark
            // given
            val path = getClass.getClassLoader.getResource("delays.csv").getPath
            val input: Dataset[AirLineDelay] = AirlineDelaysReader.read(path)

            // when
            val top_n: Dataset[CarrierDelayStats] = AirLineDelayAggs.topNCarriers(input)(2, ascending = true)

            // then
            val ds = sc.parallelize(
                Seq(
                    AirLineDelay("US", "ABE", "CLT", -10.0),
                    AirLineDelay("OO", "ABE", "ORD", 0.0),
                    AirLineDelay("EV", "ABE", "ATL", -27.0),
                    AirLineDelay("EV", "ABE", "ATL", -33.0),
                    AirLineDelay("YV", "ABE", "ORD", -2.0),
                    AirLineDelay("OO", "ABE", "ORD", -5.0),
                    AirLineDelay("FL", "ABE", "MCO", -4.0),
                    AirLineDelay("YV", "ABE", "ORD", -2.0),
                    AirLineDelay("XE", "ABE", "CLE", 17.0),
                    AirLineDelay("9E", "ABE", "DTW", -9.0)
                )
            )

            val expectedDs = ds.toDF().as[AirLineDelay]

            assertDatasetEquals(top_n, expectedDs)
        }
    }
}
