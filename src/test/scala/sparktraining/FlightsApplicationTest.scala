package sparktraining


import com.holdenkarau.spark.testing.DatasetSuiteBase
import org.apache.spark.sql.Dataset
import org.scalatest.{Matchers, WordSpec}
import sparktraining.dto.{Carrier, Flight}
import sparktraining.model.{CarrierDelayStats, CarrierFlight}
class FlightsApplicationTest extends WordSpec with Matchers with DatasetSuiteBase {

  "Flights Application" should {
    "calculate top N carriers" in {
      import spark.implicits._

      // given
      val pathCarriers = this.getClass.getClassLoader.getResource("carriers_to_join.csv").getPath
      val carriers: Dataset[Carrier] = CarriersReader.read(pathCarriers)(spark)

      val pathFlights = this.getClass.getClassLoader.getResource("delays.csv").getPath
      val flights: Dataset[Flight] = FlightsReader.read_old(pathFlights)(spark)

      // when
      val aggFlights: Dataset[CarrierDelayStats] = FlightsAggs.topNCarriers(flights, carriers)(10, ascending = true)(spark)

      // then
      val ds = sc.parallelize(
        Seq(
            CarrierDelayStats("EV", "Atlantic Southeast Airlines", -180.0, -27.0, -80.0, -33.0),
            CarrierDelayStats("US", "US Airways Inc. (Merged with America West 9/05. Reporting for both starting 10/07.)", -10.0, -10.0, -10.0, -10.0),
            CarrierDelayStats("9E", "Unknown", -9.0, -9.0, -9.0, -9.0),
            CarrierDelayStats("FL", "AirTran Airways Corporation", -4.0, -4.0, -4.0, -4.0),
            CarrierDelayStats("OO", "Skywest Airlines Inc.", -5.0, 0.0, -2.5, -2.5),
            CarrierDelayStats("YV", "Mesa Airlines Inc.", -2.0, -2.0, -2.0, -2.0),
            CarrierDelayStats("XE", "Expressjet Airlines Inc.", 17.0, 17.0, 17.0, 17.0)
        )
      )
      val expectedDs = ds.toDF().as[CarrierDelayStats]

      assertDatasetEquals(aggFlights, expectedDs)
    }

  }

}




