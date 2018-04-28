package controllers

import io.swagger.annotations._
import models.Protocol.DatasetResult
import play.api.mvc._

@Api(value = "datasets")
trait DatasetControllerT {

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
    @ApiParam(value = "hdfs, kudu", defaultValue = "hdfs", example = "hdfs, kudu") storageType: String
  ): Action[AnyContent]

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
  ): Action[AnyContent]

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
  ): Action[AnyContent]
}
