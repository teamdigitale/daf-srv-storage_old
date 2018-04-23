package it.gov.daf.dataset

import java.security.PrivilegedExceptionAction
import javax.inject.{Inject, Singleton}

import com.google.inject.ImplementedBy
import it.gov.daf.executioncontexts.WsClientExecutionContext
import it.gov.daf.utils.ParametersParser
import models.CatalogClientProtocol.StorageDataInfo
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
  /**
   *
   * @param user to user to do the action
   * @param storageType the typy of storage: hbase, mongo, kudu, textdb, hdfs
   * @param storageInfo
   * @param limit
   * @return
   */
  def dataset(user: String, storageType: String, storageInfo: StorageDataInfo, limit: Int = 1000): Future[DatasetResult]
  def schema(user: String, storageType: String, storageInfo: StorageDataInfo): Future[DatasetResult]
  def search(user: String, storageType: String, storageInfo: StorageDataInfo, query: Query): Future[DatasetResult]
}

class DatasetServiceImpl @Inject() (
  sparkEndpoint: SparkEndpoint,
  implicit val ec: WsClientExecutionContext
) extends DatasetService {

  def dataset(
    user: String,
    storageType: String,
    storageInfo: StorageDataInfo,
    limit: Int = 1000
  ): Future[DatasetResult] = withDataFrame(user, storageType, storageInfo) { df =>
      val data = df
        .limit(limit)
        .toJSON
        .collect()
      Json.parse(s"[${data.mkString(",")}]")

  }.mapToDatasetResult(user, storageType, storageInfo)

  def search(
    user: String,
    storageType: String,
    storageInfo: StorageDataInfo,
    query: Query
  ): Future[DatasetResult] = withDataFrame(user, storageType, storageInfo) { df =>
      val data = df
        .select(query.select)
        .where(query.where)
        .groupBy(query.groupBy)
        .limit(query.limit)
        .toJSON.collect()

      Json.parse(s"[${data.mkString(",")}]")

  }.mapToDatasetResult(user, storageType, storageInfo)

  def schema(
    user: String,
    storageType: String,
    storageInfo: StorageDataInfo
  ): Future[DatasetResult] = withDataFrame(user, storageType, storageInfo) { df =>
      val data = df
        .schema
        .prettyJson
      Json.parse(data)
  }.mapToDatasetResult(user, storageType, storageInfo)

  private implicit class ResultWrapper(fValue: Future[Try[JsValue]]) {
    def mapToDatasetResult(user: String, storageType: String, storageData: StorageDataInfo): Future[DatasetResult] = {
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
   * @param storageDataInfo
   * @param op
   * @tparam T
   * @return format and a valid spark session for your query
   */
  private def withDataFrame[T](
    user: String,
    storageType: String,
    storageDataInfo: StorageDataInfo
  )(op: (DataFrame) => T): Future[Try[T]] = {
    storageType.toLowerCase match {
      case "hdfs" =>
        val params = ParametersParser.asMap(storageDataInfo.storageInfo.hdfs.flatMap(_.param))
        val format = params.getOrElse("format", "parquet")

        if (!Formats.contains(format))
          Future.failed(new IllegalArgumentException(s"invalid format $format"))
        else
          sparkEndpoint.withSparkSession { spark =>
            doAsProxyUser(user, Try {
              op(spark.read.fromFormat(format, storageDataInfo.physicalUri))
            })
          }

      case other =>
        Future.failed(new IllegalArgumentException(s"$other not supported yet"))
    }
  }

  private implicit class sparkFormatterReader(reader: DataFrameReader) {
    def fromFormat(format: String, path: String): DataFrame = format match {
      case "parquet" => reader.parquet(path)
      case "csv" =>
        //This is done to deal with external csv files
        val fixAlePath = path + "/" + path.split("/").last + ".csv"
        reader.csv(fixAlePath)
      case "avro" => reader.avro(path)
      case other => reader.load() //it loads an empty DataFrame
    }
  }

}
