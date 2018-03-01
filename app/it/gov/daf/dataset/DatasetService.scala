package it.gov.daf.dataset

import java.security.PrivilegedExceptionAction
import javax.inject.{Inject, Singleton}

import com.google.inject.ImplementedBy
import it.gov.daf.executioncontexts.WsClientExecutionContext
import it.gov.daf.utils.ParametersParser
import models.CatalogClientProtocol.StorageData
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import com.databricks.spark.avro._
import models._
import org.apache.hadoop.security.UserGroupInformation
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.Future

@ImplementedBy(classOf[DatasetServiceImpl])
trait DatasetService {
  def dataset(user: String, storageType: String, storageData: StorageData, limit: Int = 1000): Future[JsValue]
  def doAsProxyUser[T](user: String)(action: => T): T
  def schema(user: String, storageType: String, storageData: StorageData): Future[JsValue]
  def search(user: String, storageType: String, storageData: StorageData, query: Query): Future[JsValue]
}

class DatasetServiceImpl @Inject() (
  sparkEndpoint: SparkEndpoint,
  implicit val ec: WsClientExecutionContext
) extends DatasetService {

  def doAsProxyUser[T](user: String)(action: => T): T = {
    val proxyUser = UserGroupInformation.createProxyUser(user, UserGroupInformation.getCurrentUser)
    proxyUser.doAs(new PrivilegedExceptionAction[T] {
      override def run(): T = action
    })
  }

  def dataset(user: String, storageType: String, storageData: StorageData, limit: Int = 1000): Future[JsValue] = {
    val fResult = executeOp(user, storageType, storageData) {
      case (format, spark) =>
        spark.read.fromFormat(format, storageData.physicalUri).limit(1000).toJSON.collect()
    }
    fResult.map(r => Json.toJson(r.mkString))
  }

  def schema(user: String, storageType: String, storageData: StorageData): Future[JsValue] = {
    val fResult = executeOp(user, storageType, storageData) {
      case (format, spark) =>
        spark.read.fromFormat(format, storageData.physicalUri).schema
    }
    fResult.map(r => Json.toJson(r.mkString))
  }

  def search(user: String, storageType: String, storageData: StorageData, query: Query): Future[JsValue] = {
    val fResult = executeOp(user, storageType, storageData) {
      case (format, spark) =>
        spark.read.fromFormat(format, storageData.physicalUri)
          .select(query.select)
          .where(query.where)
          .groupBy(query.groupBy)
          .limit(query.limit)
          .toJSON.collect()
    }
    fResult.map(r => Json.toJson(r.mkString))
  }

  /**
   * This methods handle the extraction of the data type and return the format and a valid spark session for your query
   * @param user
   * @param storageType
   * @param storageData
   * @param op
   * @tparam T
   * @return format and a valid spark session for your query
   */
  private def executeOp[T](user: String, storageType: String, storageData: StorageData)(op: (String, SparkSession) => T): Future[T] = {
    storageType.toLowerCase match {
      case "hdfs" =>
        val params = ParametersParser.asMap(storageData.storageInfo.hdfs.flatMap(_.param))
        val format = params.getOrElse("format", "parquet")

        if (!Formats.contains(format)) Future.failed(new IllegalArgumentException(s"invalid format $format"))
        else sparkEndpoint.withSparkSession { spark =>
          doAsProxyUser(user)(op(format, spark))
        }

      case other =>
        Future.failed(new IllegalArgumentException(s"$other not supported yet"))
    }
  }

  private implicit class sparkFormatterReader(reader: DataFrameReader) {
    def fromFormat(format: String, path: String): DataFrame = format match {
      case "parquet" => reader.parquet(path)
      case "csv" => reader.csv(path)
      case "avro" => reader.avro(path)
      case other => reader.load() //it loads an empty DataFrame
    }
  }

}
