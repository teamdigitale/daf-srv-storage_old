package it.gov.daf

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import it.gov.daf.dataset.{DatasetServiceImpl, SparkEndpointImpl}
import it.gov.daf.executioncontexts.WsClientExecutionContextImpl
import models.CatalogClientProtocol.{HdfsStorage, StorageData, StorageInfo}
import models.Query
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, OptionValues}
import org.scalatest.concurrent.ScalaFutures
import play.api.Configuration
import play.api.inject.DefaultApplicationLifecycle
import play.api.libs.json.{JsArray, Json}

import scala.concurrent.duration._
import scala.language.postfixOps

class DatasetServiceSpec extends FlatSpec with Matchers with BeforeAndAfterAll with ScalaFutures with OptionValues{

  val lifecycle = new DefaultApplicationLifecycle()
  val config = Configuration(ConfigFactory.load("test"))
  val endpoint = new SparkEndpointImpl(lifecycle, config)

  val system = ActorSystem("test", config.underlying)
  val ec = new WsClientExecutionContextImpl(system)

  implicit val defaultPatience =
    PatienceConfig(timeout =  10 seconds, interval = 5 millis)

  val dsService = new DatasetServiceImpl(endpoint, ec)

  "A DatasetService" should "query a parquet dataset stored into hdfs" in {

    val storageData = genHDFSStorageData(
      path = this.getClass.getResource("/parquet/userdata1.parquet").toExternalForm,
      format = "parquet"
    )
    val limit = 100

    val fResult = dsService.dataset(
      user ="test",
      storageType = "hdfs",
      storageData = storageData,
      limit = limit
    )

    whenReady(fResult){ dsResult =>
      dsResult.data.get match {
        case JsArray(array) =>
          array should have size limit

        case other =>
          fail(s"wrong result $other")
       }
    }
  }

  it should "query a avro dataset stored into hdfs" in {
    val storageData = genHDFSStorageData(
      path = this.getClass.getResource("/avro/userdata1.avro").toExternalForm,
      format = "avro"
    )
    val limit = 100

    val fResult = dsService.dataset(
      user ="test",
      storageType = "hdfs",
      storageData = storageData,
      limit = limit
    )

    whenReady(fResult){ dsResult =>
      dsResult.data.get match {
        case JsArray(array) =>
          array should have size limit

        case other =>
          fail(s"wrong result $other")
      }
    }
  }


  it should "query a csv dataset stored into hdfs" in {
    val storageData = genHDFSStorageData(
      path = this.getClass.getResource("/csv/userdata1.csv").toExternalForm,
      format = "csv"
    )
    val limit = 100

    val fResult = dsService.dataset(
      user ="test",
      storageType = "hdfs",
      storageData = storageData,
      limit = limit
    )

    whenReady(fResult){ dsResult =>
      dsResult.data.get match {
        case JsArray(array) =>
          array should have size limit

        case other =>
          fail(s"wrong result $other")
      }
    }
  }


  it should "return the schema for a parquet dataset" in {
    val storageData = genHDFSStorageData(
      path = this.getClass.getResource("/parquet/userdata1.parquet").toExternalForm,
      format = "parquet"
    )

    val fResult = dsService.schema(
      user ="test",
      storageType = "hdfs",
      storageData = storageData
    )

    whenReady(fResult){ dsResult =>
      dsResult.data.get("fields") match {
        case JsArray(array) =>
          array should have size 13

        case other =>
          fail(s"wrong result $other")
      }
    }
  }

  it should "return the schema for a avro dataset" in {
    val storageData = genHDFSStorageData(
      path = this.getClass.getResource("/avro/userdata1.avro").toExternalForm,
      format = "avro"
    )

    val fResult = dsService.schema(
      user ="test",
      storageType = "hdfs",
      storageData = storageData
    )

    whenReady(fResult){ dsResult =>
      dsResult.data.get("fields") match {
        case JsArray(array) =>
          array should have size 13

        case other =>
          fail(s"wrong result $other")
      }
    }
  }

  it should "return the schema for a csv dataset" in {
    val storageData = genHDFSStorageData(
      path = this.getClass.getResource("/csv/userdata1.csv").toExternalForm,
      format = "csv"
    )

    val fResult = dsService.schema(
      user ="test",
      storageType = "hdfs",
      storageData = storageData
    )

    whenReady(fResult){ dsResult =>
      dsResult.data.get("fields") match {
        case JsArray(array) =>
          array should have size 13

        case other =>
          fail(s"wrong result $other")
      }
    }
  }

  it should "select columns from a dataset in parquet" in {
    val storageData = genHDFSStorageData(
      path = this.getClass.getResource("/parquet/userdata1.parquet").toExternalForm,
      format = "parquet"
    )
    val limit = 100

    val fResult = dsService.search(
      user = "test",
      storageType = "hdfs",
      storageData = storageData,
      query = Query(
        select = Some(List("first_name", "last_name")),
        where = None,
        groupBy = None,
        limit = limit
      )
    )

    whenReady(fResult){ dsResult =>
      dsResult.data.get match {
        case JsArray(array) =>
          array should have size limit

        case other =>
          fail(s"wrong result $other")
      }
    }
  }

  it should "return an error when selecting wrong column names" in {
    val storageData = genHDFSStorageData(
      path = this.getClass.getResource("/parquet/userdata1.parquet").toExternalForm,
      format = "parquet"
    )
    val limit = 100
    val fResult = dsService.search(
      user = "test",
      storageType = "hdfs",
      storageData = storageData,
      query = Query(
        select = Some(List("first_name2", "last_name2")),
        where = None,
        groupBy = None,
        limit = limit
      )
    )

    whenReady(fResult){ dsResult =>
      dsResult.error should be ('defined)
    }
  }

  def genHDFSStorageData(path: String, format: String) = {
    StorageData(
      physicalUri = path,
      storageInfo = StorageInfo(
        hdfs = Some(HdfsStorage(
          name = "",
          path = None,
          param = Some(s"format=$format")
        )),
        kudu = None,
        hbase = None,
        textdb = None,
        mongo = None
      )
    )
  }


  override def afterAll(): Unit = {
    lifecycle.stop()
    system.terminate()
  }
}
