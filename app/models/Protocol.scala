package models

import play.api.libs.json.{JsValue, Json}

object Protocol {

  implicit val datasetResultWrites = Json.writes[DatasetResult]
  implicit val groupConditionWrites = Json.writes[GroupCondition]
  implicit val groupConditionReads = Json.reads[GroupCondition]

  implicit val groupByWrites = Json.writes[GroupBy]
  implicit val groupByReads = Json.reads[GroupBy]

  implicit val queryWrites = Json.writes[Query]
  implicit val queryReads = Json.reads[Query]

  case class DatasetResult(
    uri: String,
    storageType: String,
    user: String,
    data: Option[JsValue] = None,
    error: Option[String] = None
  )
}
