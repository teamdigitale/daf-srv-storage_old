package it.gov.daf

import java.net.URLEncoder

import play.api.libs.ws.WSClient

import scala.concurrent.{ExecutionContext, Future}

class CatalogClient(baseUrl: String)(private implicit val ws: WSClient) {
  import models.CatalogClientProtocol._
  /**
    *
    * @param authorization
    * @param catalogId
    * @return a StorageData containing the physical path of the dataset plut the StorageInfo
    */
  def getStorageData(authorization: String, catalogId: String)(implicit ec: ExecutionContext): Future[StorageData] = {
    val url = s"$baseUrl/catalog-manager/v1/catalog-ds/get/${URLEncoder.encode(catalogId,"UTF-8")}"
    ws.url(url)
      .addHttpHeaders("Authorization" -> authorization)
      .get()
      .map(_.json)
      .map{ response =>
        StorageData(
          physicalUri = (response \ "operational" \ "physical_uri").as[String],
          storageInfo = (response \ "operational" \ "storage_info").as[StorageInfo]
        )
      }
  }
}
