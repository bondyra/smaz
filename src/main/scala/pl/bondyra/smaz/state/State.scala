package pl.bondyra.smaz.state
import pl.bondyra.smaz.input.Input
import pl.bondyra.smaz.output.{Combiner, Output}
import pl.bondyra.smaz.processor.{Processor, ProcessorPool}

class State[I <: Input](val combiner: Combiner[I], val processorPool: ProcessorPool[I]) {
  def update(input: I): Unit = {
    processorPool.update(input)
    combiner.doWork(input, processorPool.currentOutputs)
  }

  def outputs(): Iterator[Output] = combiner.drainOutputs()
}


class StateCreator[I <: Input](val createCombiner: () => Combiner[I],
                      val processors: List[Processor[I]]
                  ){
  def newState: State[I] = {
    val processorPool: ProcessorPool[I] = ProcessorPool.create(processors)
    new State[I](createCombiner(), processorPool)
  }
}
