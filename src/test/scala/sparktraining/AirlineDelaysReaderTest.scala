package sparktraining

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import sparktraining.model.AirLineDelay

class AirlineDelaysReaderTest extends AnyWordSpec with Matchers with DatasetSuiteBase {

    private implicit val context: SparkContext = sc
    import spark.implicits._
    import sqlContext.implicits._

    "AirlineDelaysReader" should {
        "read delays csv file into dataset" in {
            // given
            val path = getClass.getClassLoader.getResource("delays.csv").getPath

            // when
            val delays: Dataset[AirLineDelay] = AirlineDelaysReader.read(path)

            // then
            val encoder = org.apache.spark.sql.Encoders.product[AirLineDelay]
            val expectedDs: Dataset[AirLineDelay] = sc.parallelize(List("", "", 3)).as(encoder)
            assertDatasetEquals(delays, expectedDs) // equal
        }
    }

}
