package pl.bondyra.smaz.processor

import pl.bondyra.smaz.config.ProcessorDefinition

class ProcessorPool private(processors: List[Processor]) {

}

object ProcessorPool {
  def create(definitions: List[ProcessorDefinition]): ProcessorPool =
    new ProcessorPool(definitions.map(processorFromDefinition))

  private def processorFromDefinition(definition: ProcessorDefinition): Processor =
    new Processor
}
