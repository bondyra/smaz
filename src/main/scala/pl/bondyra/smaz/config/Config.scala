package pl.bondyra.smaz.config


class Config {
  private[smaz] var idColumnName: String = ""
  private[smaz] var eventTimeColumnName: String = ""
  private[smaz] var outputType: OutputType = NoOutputType()
  private[smaz] var processorDefinitions: List[ProcessorDefinition] = List.empty
}

sealed trait OutputType

case class NoOutputType() extends OutputType

case class IntervalOutputType(intervalInMiliseconds: Long) extends OutputType


case class ProcessorDefinition(name: String)
