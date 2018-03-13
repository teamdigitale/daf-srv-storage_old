package it.gov.daf.dataset

import java.security.PrivilegedExceptionAction
import javax.inject.{Inject, Singleton}

import com.google.inject.ImplementedBy
import it.gov.daf.executioncontexts.WsClientExecutionContext
import it.gov.daf.utils.ParametersParser
import models.CatalogClientProtocol.StorageData
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import com.databricks.spark.avro._
import models.Protocol.DatasetResult
import models._
import org.apache.hadoop.security.UserGroupInformation
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

@ImplementedBy(classOf[DatasetServiceImpl])
trait DatasetService {
  def dataset(user: String, storageType: String, storageData: StorageData, limit: Int = 1000): Future[DatasetResult]
  def schema(user: String, storageType: String, storageData: StorageData): Future[DatasetResult]
  def search(user: String, storageType: String, storageData: StorageData, query: Query): Future[DatasetResult]
}

class DatasetServiceImpl @Inject() (
  sparkEndpoint: SparkEndpoint,
  implicit val ec: WsClientExecutionContext
) extends DatasetService {


  def dataset(
    user: String,
    storageType: String,
    storageData: StorageData,
    limit: Int = 1000
  ): Future[DatasetResult] = {
    val fResult = withSparkSession(user, storageType, storageData) {
      case (format, spark) =>
        Try {
          val data = spark.read.fromFormat(format, storageData.physicalUri)
            .limit(limit)
            .toJSON
            .collect()

          Json.parse(s"[${data.mkString(",")}]")
        }
    }
    fResult.mapToDatasetResult(user, storageType, storageData)
  }

  def search(user: String, storageType: String, storageData: StorageData, query: Query): Future[DatasetResult] = {
    val fResult = withSparkSession(user, storageType, storageData) {
      case (format, spark) =>

        Try(spark.read.fromFormat(format, storageData.physicalUri))
          .select(query.select)
          .where(query.where)
          .groupBy(query.groupBy)
          .limit(query.limit)
          .map(_.toJSON.collect())
          .map(data => Json.parse(s"[${data.mkString(",")}]"))
    }
    fResult.mapToDatasetResult(user, storageType, storageData)
  }

  def schema(user: String, storageType: String, storageData: StorageData): Future[DatasetResult] = {
    val fResult = withSparkSession(user, storageType, storageData) {
      case (format, spark) =>
        Try {
          val data = spark.read
            .fromFormat(format, storageData.physicalUri)
            .schema
            .prettyJson

          Json.parse(data)
        }
    }
    fResult.mapToDatasetResult(user, storageType, storageData)
  }

  private implicit class ResultWrapper(fValue: Future[Try[JsValue]]) {
    def mapToDatasetResult(user: String, storageType: String, storageData: StorageData): Future[DatasetResult] = {
      fValue.map {
        case Success(result) =>
          DatasetResult(
            uri = storageData.physicalUri,
            storageType = storageType,
            user = user,
            data = Some(result)
          )

        case Failure(ex) =>
          DatasetResult(
            uri = storageData.physicalUri,
            storageType = storageType,
            user = user,
            error = Some(ex.getMessage)
          )
      }
    }
  }

  /**
    *
    * @param user
    * @param action
    * @tparam T
    * @return
    */
  private def doAsProxyUser[T](user: String, action: => Try[T]): Try[T] = {
    val proxyUser = UserGroupInformation.createProxyUser(user, UserGroupInformation.getCurrentUser)
    proxyUser.doAs(new PrivilegedExceptionAction[Try[T]] {
      override def run(): Try[T] = action
    })
  }

  /**
   * This methods handle the extraction of the data type and return the format and a valid spark session for your query
    * The Spark Session is retrieved from an Object Pool stored into the SparkEndpoint
   * @param user
   * @param storageType
   * @param storageData
   * @param op
   * @tparam T
   * @return format and a valid spark session for your query
   */
  private def withSparkSession[T](
    user: String,
    storageType: String,
    storageData: StorageData
  )(op: (String, SparkSession) => Try[T]): Future[Try[T]] = {
    storageType.toLowerCase match {
      case "hdfs" =>
        val params = ParametersParser.asMap(storageData.storageInfo.hdfs.flatMap(_.param))
        val format = params.getOrElse("format", "parquet")

        if (!Formats.contains(format))
          Future.failed(new IllegalArgumentException(s"invalid format $format"))
        else
          sparkEndpoint.withSparkSession(spark => doAsProxyUser(user, op(format, spark)))

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
