/**
 * An errorhandler which wraps errors inside 
 *  json+problem https://tools.ietf.org/html/rfc7807 
 *
 * TODO Actually just uses "title" and "status".
 *      a more accurate error may be implemented eg. with 
 *      "type" and "detail", or passing a generic object to
 *      the handler. 
 */
package it.gov.daf

import play.api.http.HttpErrorHandler
import play.api.mvc._
import play.api.mvc.Results._
import play.api.libs.json.Json
import scala.concurrent._
import javax.inject.Singleton


@Singleton
class ErrorHandler extends HttpErrorHandler {

  def onClientError(request: RequestHeader, statusCode: Int, message: String) = {
    val problem = Map("title" -> Json.toJson(message), "status" -> Json.toJson(statusCode))

    Future.successful(
      Status(statusCode)(Json.toJson(problem))
    )
  }

  /**
   * Avoid exposing the full stack trace as it may contain sensitive informations.
   */ 
  def onServerError(request: RequestHeader, exception: Throwable) = {
    val problem = Map("title" -> Json.toJson(exception.getMessage), "status" -> Json.toJson(500))
    Future.successful(
      InternalServerError(Json.toJson(problem))
    )
  }
}
