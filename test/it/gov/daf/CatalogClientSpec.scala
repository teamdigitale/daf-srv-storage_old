package it.gov.daf

import mockws.{MockWS, MockWSHelpers}
import org.scalatest.BeforeAndAfterAll
import org.scalatest._
import play.api.mvc.Results._
import play.api.test.Helpers._
import play.api.libs.json._

import scala.concurrent.ExecutionContext.Implicits._

class CatalogClientSpec extends AsyncFlatSpec with Matchers with MockWSHelpers with BeforeAndAfterAll {

  val datasetJson: JsValue = Json.
    parse(this.getClass.getResourceAsStream("/standardization-dataschema.json"))

  val ws = MockWS {
    case (GET, "http://localhost:9000/catalog-manager/v1/catalog-ds/get/test") => Action { Ok(datasetJson) }
  }

  "CatalogClient" should "get the storage data" in {
    val client = new CatalogClient(baseUrl = "http://localhost:9000")(ws)
    val fResult = client.getStorageData("some authorization", "test")

    fResult.map { result =>
      assert(result.physicalUri.nonEmpty)
    }
  }

  override def afterAll(): Unit = {
    shutdownHelpers()
  }

}
