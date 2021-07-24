package sparktraining

import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.Dataset
import org.scalatest.{Matchers, WordSpec}
import sparktraining.model.{Carrier, Flight}


class CarriersReaderTest extends WordSpec with Matchers with DatasetSuiteBase {

    "CarriersReader" should {
        "read delays csv file into dataset" in {
            import spark.implicits._

            // given
            val path = this.getClass.getClassLoader.getResource("carriers.csv").getPath

            // when
            val carriers: Dataset[Carrier] = CarriersReader.read(path)(spark)

            // then
            val ds = sc.parallelize(
                Seq(
                    Carrier("02Q", "Titan Airways"),
                    Carrier("04Q", "Tradewind Aviation"),
                    Carrier("05Q", "Comlux Aviation, AG"),
                    Carrier("06Q", "Master Top Linhas Aereas Ltd."),
                    Carrier("07Q", "Flair Airlines Ltd."),
                    Carrier("08Q", "Swift Air, LLC"),
                    Carrier("0BQ", "DCA"),
                    Carrier("0CQ", "ACM AIR CHARTER GmbH"),
                    Carrier("0FQ", "Maine Aviation Aircraft Charter, LLC"),
                    Carrier("0GQ", "Inter Island Airways, d/b/a Inter Island Air")
                )
            )
            val expectedDs = ds.toDF().as[Carrier]

            assertDatasetEquals(carriers, expectedDs)
        }

    }

}
