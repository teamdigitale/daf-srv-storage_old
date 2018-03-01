package models

case class Query(
  select: Option[List[String]],
  where: Option[List[String]],
  groupBy: Option[GroupBy],
  limit: Int = 1000
)
