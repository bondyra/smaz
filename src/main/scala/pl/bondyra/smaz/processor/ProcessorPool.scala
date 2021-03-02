package pl.bondyra.smaz.processor


class ProcessorPool[I] private (processors: List[Processor[I]]) {
  def reset() = for (processor <- processors) processor.reset()
}

object ProcessorPool {
  def create[I](processors: List[Processor[I]]): ProcessorPool[I] =
    new ProcessorPool[I](processors.map(_.duplicate()))
}
