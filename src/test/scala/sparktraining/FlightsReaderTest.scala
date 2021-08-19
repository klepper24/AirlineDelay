package sparktraining

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.Dataset
import org.scalatest.{Matchers, WordSpec}
import sparktraining.dto.Flight
import sparktraining.model.CarrierDelayStats


class FlightsReaderTest extends WordSpec with Matchers with DatasetSuiteBase {

    "FlightsReader" should {
        "read delays csv file into dataset" in {
            import spark.implicits._

            // given
            val path = this.getClass.getClassLoader.getResource("delays.csv").getPath

            // when
            val delays: Dataset[Flight] = FlightsReader.read(path)(spark)

            // then
            val ds = sc.parallelize(
                Seq(
                    Flight("US", "ABE", "CLT", -10.0),
                    Flight("OO", "ABE", "ORD", 0.0),
                    Flight("EV", "ABE", "ATL", -27.0),
                    Flight("EV", "ABE", "ATL", -33.0),
                    Flight("EV", "ABE", "ATL", -180.0),
                    Flight("YV", "ABE", "ORD", -2.0),
                    Flight("OO", "ABE", "ORD", -5.0),
                    Flight("FL", "ABE", "MCO", -4.0),
                    Flight("YV", "ABE", "ORD", -2.0),
                    Flight("XE", "ABE", "CLE", 17.0),
                    Flight("9E", "ABE", "DTW", -9.0)
                )
            )
            val expectedDs = ds.toDF().as[Flight]

            assertDatasetEquals(delays, expectedDs)
        }

        "read delays csv file with additional columns into dataset" in {
            import spark.implicits._

            // given
            val path = this.getClass.getClassLoader.getResource("delays_with_cols.csv").getPath

            // when
            val delays: Dataset[Flight] = FlightsReader.read(path)(spark)

            // then
            val ds = sc.parallelize(
                Seq(
                    Flight("US", "ABE", "CLT", -10.0),
                    Flight("OO", "ABE", "ORD", 0.0),
                    Flight("EV", "ABE", "ATL", -27.0),
                    Flight("EV", "ABE", "ATL", -33.0),
                    Flight("EV", "ABE", "ATL", -180.0),
                    Flight("YV", "ABE", "ORD", -2.0),
                    Flight("OO", "ABE", "ORD", -5.0),
                    Flight("FL", "ABE", "MCO", -4.0),
                    Flight("YV", "ABE", "ORD", -2.0),
                    Flight("XE", "ABE", "CLE", 17.0),
                    Flight("9E", "ABE", "DTW", -9.0)
                )
            )
            val expectedDs = ds.toDF().as[Flight]

            assertDatasetEquals(delays, expectedDs)
        }
    }

}
