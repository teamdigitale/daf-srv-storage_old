package models

import play.api.libs.json._

object CatalogClientProtocol {

  case class StorageData(
    physicalUri: String,
    storageInfo: StorageInfo
  )

  implicit val hdfsStorageReads = Json.reads[HdfsStorage]
  implicit val kuduStorageReads = Json.reads[KuduStorage]
  implicit val hbaseStorageReads = Json.reads[HBaseStorage]
  implicit val textStorageReads = Json.reads[TextStorage]
  implicit val mongoStorageReads = Json.reads[MongoStorage]
  implicit val storageInfoReads = Json.reads[StorageInfo]

  case class StorageInfo(
    hdfs: Option[HdfsStorage],
    kudu: Option[KuduStorage],
    hbase: Option[HBaseStorage],
    textdb: Option[TextStorage],
    mongo: Option[MongoStorage]
  )

  case class HdfsStorage(
    name: String,
    path: Option[String],
    param: Option[String]
  )

  case class KuduStorage(
    name: String,
    tableName: Option[String],
    param: Option[String]
  )

  case class HBaseStorage(
    name: String,
    metric: Option[String],
    tags: List[String]
  )

  case class TextStorage(
    name: String,
    path: String,
    param: String
  )

  case class MongoStorage(
    name: String,
    path: Option[String],
    param: Option[String]
  )

}

