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
    //import sqlContext.implicits._ zakomentowalem, bo ta linijka psula kompilacje

    "AirlineDelaysReader" should {
        "read delays csv file into dataset" in {
            // given
            val path = getClass.getClassLoader.getResource("delays.csv").getPath

            // when
            val delays: Dataset[AirLineDelay] = AirlineDelaysReader.read(path)

            // then
            val ds = sc.parallelize(Seq(AirLineDelay("DCA", "EWR", 4.0), AirLineDelay("EWR", "IAD", -8.0), AirLineDelay("EWR", "DCA", -9.0), AirLineDelay("DCA", "EWR", -12.0), AirLineDelay("IAD", "EWR", -38.0)))
            val expectedDs = ds.toDS()
            assertDatasetEquals(delays, expectedDs) // equal
        }
    }

}
