package sparktraining


import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.Dataset
import org.scalatest.{Matchers, WordSpec}
import sparktraining.model.{CarrierDict, CarrierFlight, Flight}
class CarrierFlightJoinTest extends WordSpec with Matchers with DatasetSuiteBase {

  "CarrierFlightJoinTest" should {
    "join carriers dictionary with flights" in {
      import spark.implicits._

      // given
      val pathCarriers = this.getClass.getClassLoader.getResource("carriers_to_join.csv").getPath
      val pathFlights = this.getClass.getClassLoader.getResource("delays.csv").getPath

      // when
      val carrierDict: Dataset[CarrierDict] = CarriersReader.read(pathCarriers)(spark)
      val flights: Dataset[Flight] = FlightsReader.read(pathFlights)(spark)
      val carrierFlights: Dataset[CarrierFlight] = CarrierDictFlightsJoin.joinFunc(carrierDict, flights)(spark)

      // then
      val ds = sc.parallelize(
        Seq(
          CarrierFlight("US Airways Inc. (Merged with America West 9/05. Reporting for both starting 10/07.)", "ABE", "CLT", -10.0),
          CarrierFlight("Skywest Airlines Inc.", "ABE", "ORD", 0.0),
          CarrierFlight("Atlantic Southeast Airlines", "ABE", "ATL", -27.0),
          CarrierFlight("Atlantic Southeast Airlines", "ABE", "ATL", -33.0),
          CarrierFlight("Atlantic Southeast Airlines", "ABE", "ATL", -180.0),
          CarrierFlight("Mesa Airlines Inc.", "ABE", "ORD", -2.0),
          CarrierFlight("Skywest Airlines Inc.", "ABE", "ORD", -5.0),
          CarrierFlight("AirTran Airways Corporation", "ABE", "MCO", -4.0),
          CarrierFlight("Mesa Airlines Inc.", "ABE", "ORD", -2.0),
          CarrierFlight("Expressjet Airlines Inc.", "ABE", "CLE", 17.0),
          CarrierFlight("Pinnacle Airlines Inc.", "ABE", "DTW", -9.0)
        )
      )
      val expectedDs = ds.toDF().as[CarrierFlight]

      assertDatasetEquals(carrierFlights, expectedDs)
    }

  }

}




