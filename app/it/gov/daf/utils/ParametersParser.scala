package it.gov.daf.utils

object ParametersParser {

  val elementSeparator = ","
  val keyValueSeparator = "="

  def asMap(sParams: Option[String]): Map[String, String] = {
    sParams.map(_.split(elementSeparator))
      .map {array =>

        array.map(_.split(keyValueSeparator))
            .filter(_.length == 2)
          .map{
            case Array(k,v) =>
              k -> v
          }.toMap
      }.getOrElse(Map.empty)
  }
}
