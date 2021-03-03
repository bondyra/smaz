package pl.bondyra.smaz.processor

import pl.bondyra.smaz.input.Input


class ProcessorPool[I <: Input] private (processors: List[Processor[I]]) {
  def update(input: I): Unit = {
    for (processor <- processors)
      processor.update(input)
  }

  def currentOutputs: Map[String, String] = processors.map(p => (p.name, p.stringValue)).toMap

  def reset() = for (processor <- processors) processor.reset()
}

object ProcessorPool {
  def create[I <: Input](processors: List[Processor[I]]): ProcessorPool[I] =
    new ProcessorPool[I](processors.map(_.duplicate()))
}
