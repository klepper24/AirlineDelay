package sparktraining

import java.io.File

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import sparktraining.model.AirLineDelay

class AirLineDelayAggsTest extends AnyWordSpec with Matchers with DatasetSuiteBase {

    private implicit val context: SparkContext = sc

    "AirLineDelayAggs" should {
        "calculate top N carriers" in {
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
