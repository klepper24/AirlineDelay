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
                    CarrierDelayStats("US", -10.0, -10.0),//, -10.0, -10.0),
                    CarrierDelayStats("OO", 0.0, -5.0),//, -2.5, -2.5),
                    CarrierDelayStats("EV", -27.0, -33.0),//, -30.0, -30.0),
                    CarrierDelayStats("YV", -2.0, -2.0),//, -2.0, -2.0),
                    CarrierDelayStats("FL", -4.0, -4.0),//, -4.0, -4.0),
                    CarrierDelayStats("XE", 17.0, 17.0),//, 17.0, 17.0),
                    CarrierDelayStats("9E", -9.0, -9.0)//, -9.0, -9.0)
                )
            )
            val expectedDs = ds.toDF().as[CarrierDelayStats]

            assertDatasetEquals(delays, expectedDs)
        }
    }

}
