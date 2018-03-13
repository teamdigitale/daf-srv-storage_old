package it.gov.daf.dataset

object Formats extends Enumeration {
  type Formats = Value
  val csv, parquet, avro = Value

  private val sValues = Formats.values.map(_.toString).toSet

  def contains(value: String): Boolean = sValues.contains(value)

}
