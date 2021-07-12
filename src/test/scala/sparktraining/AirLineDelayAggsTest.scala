package sparktraining

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{Matchers, WordSpec}
import sparktraining.model.AirLineDelay

class AirLineDelayAggsTest extends WordSpec with Matchers with DatasetSuiteBase {

    "AirLineDelayAggs" should {
        "calculate top N carriers" in {
            implicit val sparkSession: SparkSession = spark
            // given
            val path = getClass.getClassLoader.getResource("delays.csv").getPath
            val input: Dataset[AirLineDelay] = AirlineDelaysReader.read(path)

            // when
            AirLineDelayAggs.topNCarriers(input)(2, ascending = true)

            // then
            true should be(true)
        }
    }
}
