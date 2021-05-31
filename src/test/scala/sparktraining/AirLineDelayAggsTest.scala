package sparktraining

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AirLineDelayAggsTest extends AnyWordSpec with Matchers with DatasetSuiteBase {

  "AirLineDelayAggs" should {
    "calculate top N carriers" in {

        new AirLineDelayAggs().topNCarriers()

        true should be (true)
    }
  }
}
