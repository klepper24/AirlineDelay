package sparktraining

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.Dataset
import org.scalatest.{Matchers, WordSpec}
import sparktraining.model.{AirLineDelay, CarrierDelayStats}


class AirlineDelaysReaderTest extends WordSpec with Matchers with DatasetSuiteBase {

    "AirlineDelaysReader" should {
        "read delays csv file into dataset" in {
            import spark.implicits._

            // given
            val path = this.getClass.getClassLoader.getResource("delays.csv").getPath

            // when
            val delays: Dataset[AirLineDelay] = AirlineDelaysReader.read(path)(spark)

            // then
            val ds = sc.parallelize(
                Seq(
                    AirLineDelay("US", "ABE", "CLT", -10.01),
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

            assertDatasetEquals(delays, expectedDs)
        }
    }

}
