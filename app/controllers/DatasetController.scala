package controllers

import javax.inject.Inject

import io.swagger.annotations._
import play.api.Configuration
import play.api.libs.json._
import play.api.mvc._

@Api(value = "dataset")
class DatasetController @Inject() (
  config: Configuration,
  cc: ControllerComponents
) extends AbstractController(cc) {

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
  def dataset(uri: String) = Action { request =>
    val json = Json.parse(
      """
          |{
          | "response" : "test"
          |}
        """.stripMargin
    )
    Ok(json)
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
  def schema(uri: String) = Action { request =>
    Ok("it works!")
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
  def search(uri: String) = Action { request =>
    Ok("it works!")
  }
}
