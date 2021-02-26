package pl.bondyra.smaz.output

case class Output (
  identifier: String,
  eventTime: Double,
  version: Long,
  values: Map[String, String]  // TODO reconsider this
                  // in java I have hardcoded this name ("values") upstream - in "Engine" equivalent
                  // I shouldn't do it though
                  // I need to find a smarter way to map this class (with hidden dynamic processor output in Map[String, String]
                  // into a dynamic spark row
)

object Output {
  def columnsToSelect: Iterable[String] = {
    Seq("identifier", "eventTime", "version") ++ values.map(kv => s"values.${kv._1}")
  }
}