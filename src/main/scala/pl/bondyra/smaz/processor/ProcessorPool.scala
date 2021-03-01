package pl.bondyra.smaz.processor


class ProcessorPool private (processors: List[Processor]) {

}

object ProcessorPool {
  def create(processors: List[Processor]): ProcessorPool =
    new ProcessorPool(processors.map(p => p.duplicate().asInstanceOf[p.getClass]))
}
