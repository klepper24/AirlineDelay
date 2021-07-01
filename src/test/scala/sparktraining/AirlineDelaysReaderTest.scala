package sparktraining

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import sparktraining.model.AirLineDelay


class AirlineDelaysReaderTest extends AnyWordSpec with Matchers with DatasetSuiteBase {
    
    "AirlineDelaysReader" should {
        "read delays csv file into dataset" in {
            import spark.implicits._

            // given
            val path = this.getClass.getClassLoader.getResource("delays.csv").getPath

            // when
            val delays: Dataset[AirLineDelay] = AirlineDelaysReader.read(path)(spark)

            // then
            val ds = sc.parallelize(Seq(AirLineDelay("DCA", "EWR", 4.0), AirLineDelay("EWR", "IAD", -8.0), AirLineDelay("EWR", "DCA", -9.0), AirLineDelay("DCA", "EWR", -12.0), AirLineDelay("IAD", "EWR", -38.0)))
            val expectedDs = ds.toDF().as[AirLineDelay]

            //assertDatasetEquals(delays, expectedDs) // equal
            delays.show()
            expectedDs.show()
        }
    }

}
