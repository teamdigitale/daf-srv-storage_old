package it.gov.daf.dataset

import javax.inject.Inject

import com.google.inject.ImplementedBy
import it.gov.daf.executioncontexts.WsClientExecutionContext
import it.gov.daf.utils.ParametersParser
import models.CatalogClientProtocol.StorageData
import org.apache.spark.sql.DataFrame
import com.databricks.spark.avro._
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.Future

@ImplementedBy(classOf[DatasetServiceImpl])
trait DatasetService {
  def dataset(proxyUser: String, storageType: String, storageData: StorageData, limit: Int = 1000): Future[JsValue]
}

class DatasetServiceImpl @Inject() (
  sparkEndpoint: SparkEndpoint,
  implicit val ec: WsClientExecutionContext
) extends DatasetService {

  def dataset(proxyUser: String, storageType: String, storageData: StorageData, limit: Int = 1000): Future[JsValue] = {
    val futureDF = storageType.toLowerCase match {
      case "hdfs" =>
        val params = ParametersParser.asMap(storageData.storageInfo.hdfs.flatMap(_.param))
        val format = params.getOrElse("format", "parquet")

        if (Formats.contains(format)){
          sparkEndpoint.reserveSparkSession.map { spark =>

            format match {
              case "parquet" => spark.read.parquet(storageData.physicalUri).limit(1000)
              case "csv" => spark.read.csv(storageData.physicalUri).limit(1000)
              case "avro" => spark.read.avro(storageData.physicalUri).limit(1000)
              case other => spark.emptyDataFrame
            }
          }
        } else Future.failed[DataFrame](new IllegalArgumentException(s"invalid format $format"))

      case other =>
        Future.failed[DataFrame](new IllegalArgumentException(s"$other not supported yet"))
    }

    //to Json
    futureDF
      .map(df => Json.toJson(df.toJSON.collect().mkString))
  }

}
