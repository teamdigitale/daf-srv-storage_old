package it.gov.daf

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import it.gov.daf.executioncontexts.WsClientExecutionContextImpl
import mockws.{MockWS, MockWSHelpers}
import org.scalatest.BeforeAndAfterAll
import org.scalatest._
import play.api.Configuration
import play.api.mvc.Results._
import play.api.test.Helpers._
import play.api.libs.json._

import scala.concurrent.ExecutionContext.Implicits._

class CatalogClientSpec extends AsyncFlatSpec with Matchers with MockWSHelpers with BeforeAndAfterAll {

  val datasetJson: JsValue = Json.
    parse(this.getClass.getResourceAsStream("/standardization-dataschema.json"))


  val ws = mockws.MockWS {
    case (GET, "http://localhost:9001/catalog-manager/v1/catalog-ds/get/test") => Action { Ok(datasetJson) }
  }

  val config = ConfigFactory.load("test")
  val system = ActorSystem("test", config)
  val ec = new WsClientExecutionContextImpl(system)

  "CatalogClient" should "get the storage data" in {

    val client = new CatalogClientImpl(Configuration(config), ws, ec)
    val fResult = client.getStorageData("some authorization", "test")

    fResult.map { result =>
      assert(result.physicalUri.nonEmpty)
    }
  }

  override def afterAll(): Unit = {
    shutdownHelpers()
    system.terminate()
  }

}
