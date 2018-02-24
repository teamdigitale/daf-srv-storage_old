package models

import play.api.libs.json.{JsValue, Json}

object Protocol {

  implicit val datasetResultWrites = Json.writes[DatasetResult]

  case class DatasetResult(
    uri: String,
    storageType: String,
    user: String,
    data: Option[JsValue] = None,
    error: Option[JsValue] = None)
}
