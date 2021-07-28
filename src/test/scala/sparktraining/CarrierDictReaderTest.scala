package sparktraining

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.Dataset
import org.scalatest.{Matchers, WordSpec}
import sparktraining.model.CarrierDict


class CarrierDictReaderTest extends WordSpec with Matchers with DatasetSuiteBase {

    "CarriersReader" should {
        "read delays csv file into dataset" in {
            import spark.implicits._

            // given
            val path = this.getClass.getClassLoader.getResource("carriers.csv").getPath

            // when
            val carriers: Dataset[CarrierDict] = CarriersReader.read(path)(spark)

            // then
            val ds = sc.parallelize(
                Seq(
                    CarrierDict("02Q", "Titan Airways"),
                    CarrierDict("04Q", "Tradewind Aviation"),
                    CarrierDict("05Q", "Comlux Aviation, AG"),
                    CarrierDict("06Q", "Master Top Linhas Aereas Ltd."),
                    CarrierDict("07Q", "Flair Airlines Ltd."),
                    CarrierDict("09Q", "Swift Air, LLC"),
                    CarrierDict("0BQ", "DCA"),
                    CarrierDict("0CQ", "ACM AIR CHARTER GmbH"),
                    CarrierDict("0FQ", "Maine Aviation Aircraft Charter, LLC"),
                    CarrierDict("0GQ", "Inter Island Airways, d/b/a Inter Island Air")
                )
            )
            val expectedDs = ds.toDF().as[CarrierDict]
            carriers.show()
            assertDatasetEquals(carriers, expectedDs)
        }

    }

}
