package controllers

import javax.inject.Inject

import io.swagger.annotations._
import it.gov.daf.executioncontexts.WsClientExecutionContext
import it.gov.daf.CatalogClient
import it.gov.daf.dataset.DatasetService
import models._
import models.CatalogClientProtocol.StorageDataInfo
import models.Protocol._
import play.api.Configuration
import play.api.libs.json._
import play.api.libs.ws._
import play.api.mvc._

import scala.concurrent.Future

@Api(value = "datasets")
class DatasetController @Inject() (
  config: Configuration,
  cc: ControllerComponents,
  datasetService: DatasetService,
  catalogClient: CatalogClient,
  implicit val ec: WsClientExecutionContext,
  implicit val ws: WSClient
) extends AbstractController(cc) with DatasetControllerT {

  /**
   * Given
   * 1. an authenticated request
   * 2. extract the user (pac4j)
   * 3. extract the need information from the dataset
   * 4. execute the query using the proxy user
   * 5. return the result
   *
   * @param uri
   * @return
   */
  def dataset(uri: String, storageType: String) = Action.async { request =>
    request.headers.get("Authorization") match {
      case Some(auth) =>
        val user = extractUsername(auth)
        catalogClient.getStorageDataInfo(auth, uri)
          .flatMap(datasetService.dataset(user, storageType, _))
          .map(r => Ok(Json.toJson(r)))

      case None =>
        Future.successful(BadRequest("Invalid Authorization"))
    }
  }

  def schema(uri: String, storageType: String) = Action.async { request =>
    request.headers.get("Authorization") match {
      case Some(auth) =>
        val user = extractUsername(auth)
        catalogClient.getStorageDataInfo(auth, uri)
          .flatMap(datasetService.schema(user, storageType, _))
          .map(r => Ok(Json.toJson(r)(datasetResultWrites)))

      case None =>
        Future.successful(BadRequest("Invalid Authorization"))
    }
  }

  def search(uri: String, storageType: String) = Action.async { request =>
    request.headers.get("Authorization") match {
      case Some(auth) =>
        val user = extractUsername(auth)

        request.body.asJson.map(_.as[Query]) match {
          case Some(query) =>
            catalogClient.getStorageDataInfo(auth, uri)
              .flatMap(datasetService.search(user, storageType, _, query))
              .map(r => Ok(Json.toJson(r)))

          case None =>
            Future.successful(BadRequest("Missing Query Body"))
        }

      case None =>
        Future.successful(BadRequest("Invalid Authorization"))
    }
  }

  private def extractUsername(s: String): String = ???
}
