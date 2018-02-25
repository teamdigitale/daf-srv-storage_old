package it.gov.daf.dataset

import java.security.PrivilegedExceptionAction
import javax.inject.{Inject, Singleton}

import com.google.inject.ImplementedBy
import it.gov.daf.executioncontexts.WsClientExecutionContext
import it.gov.daf.utils.ParametersParser
import models.CatalogClientProtocol.StorageData
import org.apache.spark.sql.DataFrame
import com.databricks.spark.avro._
import org.apache.hadoop.security.UserGroupInformation
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.Future

@ImplementedBy(classOf[DatasetServiceImpl])
trait DatasetService {
  def dataset(proxyUser: String, storageType: String, storageData: StorageData, limit: Int = 1000): Future[JsValue]
  def doAsProxyUser(user: String)(action: => DataFrame): DataFrame
}

class DatasetServiceImpl @Inject() (
  sparkEndpoint: SparkEndpoint,
  implicit val ec: WsClientExecutionContext
) extends DatasetService {

  def doAsProxyUser(user: String)(action: => DataFrame): DataFrame = {
    val proxyUser = UserGroupInformation.createProxyUser(user, UserGroupInformation.getCurrentUser)
    proxyUser.doAs(new PrivilegedExceptionAction[DataFrame] {
      override def run(): DataFrame = action
    })
  }

  def dataset(proxyUser: String, storageType: String, storageData: StorageData, limit: Int = 1000): Future[JsValue] = {
    val fResult: Future[Array[String]] = storageType.toLowerCase match {
      case "hdfs" =>
        val params = ParametersParser.asMap(storageData.storageInfo.hdfs.flatMap(_.param))
        val format = params.getOrElse("format", "parquet")

        if (Formats.contains(format)){
          sparkEndpoint.withSparkSession{ spark =>
            val result = doAsProxyUser(proxyUser){
              format match {
                case "parquet" => spark.read.parquet(storageData.physicalUri).limit(1000)
                case "csv" => spark.read.csv(storageData.physicalUri).limit(1000)
                case "avro" => spark.read.avro(storageData.physicalUri).limit(1000)
                case other => spark.emptyDataFrame
              }
            }
            result.toJSON.collect()
          }
        } else Future.failed[Array[String]](new IllegalArgumentException(s"invalid format $format"))

      case other =>
        Future.failed[Array[String]](new IllegalArgumentException(s"$other not supported yet"))
    }
    //transform to JsValue
    fResult.map(r => Json.toJson(r.mkString))
  }

}
