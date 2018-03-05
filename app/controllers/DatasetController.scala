package controllers

import javax.inject.Inject

import io.swagger.annotations._
import it.gov.daf.executioncontexts.WsClientExecutionContext
import it.gov.daf.CatalogClient
import it.gov.daf.dataset.DatasetService
import models._
import models.CatalogClientProtocol.StorageData
import models.Protocol._
import play.api.Configuration
import play.api.libs.json._
import play.api.libs.ws._
import play.api.mvc._

import scala.concurrent.Future

@Api(value = "dataset")
class DatasetController @Inject() (
  config: Configuration,
  cc: ControllerComponents,
  datasetService: DatasetService,
  catalogClient: CatalogClient,
  implicit val ec: WsClientExecutionContext,
  implicit val ws: WSClient
) extends AbstractController(cc) {

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
  @ApiOperation(
    value = "Get a dataset based on the dataset id",
    produces = "application/json",
    httpMethod = "GET",
    authorizations = Array(new Authorization(value = "basicAuth")),
    protocols = "https, http",
    response = classOf[DatasetResult]
  )
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Invalid ID supplied"),
    new ApiResponse(code = 404, message = "Dataset not found")
  ))
  def dataset(
    @ApiParam(value = "the unique name of the dataset", defaultValue = "") uri: String,
    @ApiParam(value = "hdfs, kudu", defaultValue = "hdfs") storageType: String
  ) = Action.async { request =>
    request.headers.get("Authorization") match {
      case Some(auth) =>
        val user = extractUsername(auth)
        catalogClient.getStorageData(auth, uri)
          .flatMap { sd =>
            datasetService
              .dataset(user, storageType, sd)
          }
          .map(r => Ok(Json.toJson(r)))

      case None =>
        Future.successful(BadRequest("Invalid Authorization"))
    }
  }

  @ApiOperation(
    value = "Get a dataset based on the dataset id",
    produces = "application/json",
    httpMethod = "GET",
    authorizations = Array(new Authorization(value = "basicAuth")),
    protocols = "https, http"
  )
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Invalid ID supplied"),
    new ApiResponse(code = 404, message = "Dataset not found")
  ))
  def schema(
    @ApiParam(value = "the unique name of the dataset", defaultValue = "") uri: String,
    @ApiParam(value = "hdfs, kudu", defaultValue = "hdfs") storageType: String
  ) = Action.async { request =>
    request.headers.get("Authorization") match {
      case Some(auth) =>
        val user = extractUsername(auth)
        catalogClient.getStorageData(auth, uri)
          .flatMap { sd =>
            datasetService.schema(user, storageType, sd)
          }
          .map(r => Ok(Json.toJson(r)(datasetResultWrites)))

      case None =>
        Future.successful(BadRequest("Invalid Authorization"))
    }
  }

  @ApiOperation(
    value = "Get a dataset based on the dataset id",
    produces = "application/json",
    httpMethod = "GET",
    authorizations = Array(new Authorization(value = "basicAuth")),
    protocols = "https, http"
  )
  @ApiImplicitParams(Array(
    new ApiImplicitParam(
      name = "query",
      value = "A valid query",
      required = true,
      dataType = "models.Query",
      paramType = "body"
    )
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Invalid ID supplied"),
    new ApiResponse(code = 404, message = "Dataset not found")
  ))
  def search(
    @ApiParam(value = "the unique name of the dataset", defaultValue = "") uri: String,
    @ApiParam(value = "hdfs, kudu", defaultValue = "hdfs") storageType: String
  ) = Action.async { request =>
    request.headers.get("Authorization") match {
      case Some(auth) =>
        val user = extractUsername(auth)
        request.body.asJson.map(_.as[Query]) match {
          case Some(query) =>
            catalogClient.getStorageData(auth, uri)
              .flatMap { sd =>
                datasetService.search(user, storageType, sd, query)
              }
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
