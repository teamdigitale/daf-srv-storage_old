package models

case class GroupBy(
  groupColumn: String,
  conditions: List[GroupCondition]
)

