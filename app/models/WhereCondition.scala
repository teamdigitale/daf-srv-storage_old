package models

import scala.util.{Failure, Success, Try}

/**
 *
 * @param property nome of attribute
 * @param symbol > < != ...
 * @param value value to be checked
 */
case class WhereCondition(
  property: String,
  symbol: String,
  value: String
) {
  /**
    *
    * @return
    */
  def toSql(): String = {

    Try(value.toDouble) match {
      case Success(parsed) =>
        s"$property $symbol $parsed"

      case Failure(_) =>
        s"$property $symbol '$value'"
    }


  }
}


