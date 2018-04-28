package it.gov.daf

import java.net.URLEncoder
import javax.inject.{Inject, Singleton}

import com.google.inject.ImplementedBy
import it.gov.daf.executioncontexts.WsClientExecutionContext
import models.CatalogClientProtocol.StorageDataInfo
import play.api.Configuration
import play.api.libs.ws.WSClient

import scala.concurrent.Future

@ImplementedBy(classOf[CatalogClientImpl])
trait CatalogClient {
  def getStorageDataInfo(authorization: String, catalogId: String): Future[StorageDataInfo]
}

@Singleton
class CatalogClientImpl @Inject() (
  config: Configuration,
  implicit val ws: WSClient,
  implicit val ec: WsClientExecutionContext
) extends CatalogClient {
  import models.CatalogClientProtocol._

  private val baseUrl = config.get[String]("catalog-manager.base-url")
  /**
   *
   * @param authorization
   * @param catalogId
   * @return a StorageData containing the physical path of the dataset plut the StorageInfo
   */
  def getStorageDataInfo(authorization: String, catalogId: String): Future[StorageDataInfo] = {
    val url = s"$baseUrl/catalog-manager/v1/catalog-ds/get/${URLEncoder.encode(catalogId, "UTF-8")}"
    ws.url(url)
      .addHttpHeaders("Authorization" -> authorization)
      .get()
      .map(_.json)
      .map { response =>
        StorageDataInfo(
          physicalUri = (response \ "operational" \ "physical_uri").as[String],
          storageInfo = (response \ "operational" \ "storage_info").as[StorageInfo]
        )
      }
  }
}
