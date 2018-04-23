package it.gov.daf

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import it.gov.daf.dataset.{DatasetServiceImpl, SparkEndpointImpl}
import it.gov.daf.executioncontexts.WsClientExecutionContextImpl
import models.CatalogClientProtocol.{HdfsStorage, StorageDataInfo, StorageInfo}
import models.{Query, WhereCondition}
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

  val parquetStorageData = genHDFSStorageData(
    path = this.getClass.getResource("/parquet/userdata1.parquet").toExternalForm,
    format = "parquet"
  )

  val avroStorageData = genHDFSStorageData(
    path = this.getClass.getResource("/avro/userdata1.avro").toExternalForm,
    format = "avro"
  )

  val csvStorageData = genHDFSStorageData(
    path = this.getClass.getResource("/csv/userdata1").toExternalForm,
    format = "csv"
  )

  "A DatasetService" should "query a parquet dataset stored into hdfs" in {

    val limit = 100

    val fResult = dsService.dataset(
      user ="test",
      storageType = "hdfs",
      storageInfo = parquetStorageData,
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
    val limit = 100

    val fResult = dsService.dataset(
      user ="test",
      storageType = "hdfs",
      storageInfo = avroStorageData,
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
    val limit = 100

    val fResult = dsService.dataset(
      user ="test",
      storageType = "hdfs",
      storageInfo = csvStorageData,
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

    val fResult = dsService.schema(
      user ="test",
      storageType = "hdfs",
      storageInfo = parquetStorageData
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
    val fResult = dsService.schema(
      user ="test",
      storageType = "hdfs",
      storageInfo = avroStorageData
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
    val fResult = dsService.schema(
      user ="test",
      storageType = "hdfs",
      storageInfo = csvStorageData
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

  it should "SELECT columns from a dataset in parquet" in {
    val limit = 100

    val fResult = dsService.search(
      user = "test",
      storageType = "hdfs",
      storageInfo = parquetStorageData,
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

  it should "return an error when SELECTING wrong column names" in {
    val limit = 100
    val fResult = dsService.search(
      user = "test",
      storageType = "hdfs",
      storageInfo = parquetStorageData,
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

  it should "perform WHERE on string value" in {
    val limit = 100

    val fResult = dsService.search(
      user = "test",
      storageType = "hdfs",
      storageInfo = parquetStorageData,
      query = Query(
        select = None,
        where = Some(List(WhereCondition(
          property = "first_name",
          symbol = "==",
          value = "Amanda"
        ))),
        groupBy = None,
        limit = limit
      )
    )

    whenReady(fResult) { dsResult =>
      dsResult.data.get match {
        case JsArray(array) =>
          array should have size 7

        case other =>
          fail(s"wrong result $other")
      }
    }
  }

    it should "perform WHERE on double value" in {
      val limit = 100

      val fResult = dsService.search(
        user = "test",
        storageType = "hdfs",
        storageInfo = parquetStorageData,
        query = Query(
          select = None,
          where = Some(List(WhereCondition(
            property = "salary",
            symbol = ">",
            value = "280000"
          ))),
          groupBy = None,
          limit = limit
        )
      )

      whenReady(fResult){ dsResult =>
        dsResult.data.get match {
          case JsArray(array) =>
            array should have size 18

          case other =>
            fail(s"wrong result $other")
        }
      }
  }

  it should "perform WHERE on date value" in {
    val limit = 100

    val fResult = dsService.search(
      user = "test",
      storageType = "hdfs",
      storageInfo = parquetStorageData,
      query = Query(
        select = None,
        where = Some(List(WhereCondition(
          property = "birthdate",
          symbol = "==",
          value = "3/8/1971"
        ))),
        groupBy = None,
        limit = limit
      )
    )

    whenReady(fResult){ dsResult =>
      dsResult.data.get match {
        case JsArray(array) =>
          array should have size 1

        case other =>
          fail(s"wrong result $other")
      }
    }
  }

  def genHDFSStorageData(path: String, format: String) = {
    StorageDataInfo(
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
