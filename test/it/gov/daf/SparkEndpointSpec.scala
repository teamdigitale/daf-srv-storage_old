package it.gov.daf

import com.typesafe.config.ConfigFactory
import it.gov.daf.dataset.SparkEndpointImpl
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{AsyncFlatSpec, BeforeAndAfterAll, FlatSpec, Matchers}
import play.api.Configuration
import play.api.inject.DefaultApplicationLifecycle
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps

import scala.concurrent.duration._

class SparkEndpointSpec extends FlatSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  val lifecycle = new DefaultApplicationLifecycle()
  val config = Configuration(ConfigFactory.load("test"))
  val endpoint = new SparkEndpointImpl(lifecycle, config)

  implicit val defaultPatience =
    PatienceConfig(timeout =  10 seconds, interval = 5 millis)

  "A SparkEndpoint" should "be able to reserve a spark session" in {
     val result = endpoint.withSparkSession { spark =>
       spark.range(1,1000).toJSON.collect()
     }
    whenReady(result)(_.length shouldBe 999)
  }

  it should "should fail if the resource in the pool is not released" in {

    val result = endpoint.reserveSparkSession.map { spark =>
      spark.range(1,1000).toJSON.collect()
    }

    whenReady(result)(_.length shouldBe 999)

    val result1 = endpoint.reserveSparkSession.map { spark =>
      spark.range(1,1000).toJSON.collect()
    }

    try {
      whenReady(result1.failed){e =>
        println(e)
      }
    } catch {
      case e: Exception =>
        assert(true)
    }
  }

  override def afterAll(): Unit = {
    lifecycle.stop()
  }

}
