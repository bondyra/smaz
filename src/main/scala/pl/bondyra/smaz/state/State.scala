package pl.bondyra.smaz.state
import pl.bondyra.smaz.output.{Output, OutputStrategy}
import pl.bondyra.smaz.processor.{Processor, ProcessorPool}

import scala.collection.mutable

class State[I] (val outputStrategy: OutputStrategy[I], val processorPool: ProcessorPool[I]) {
  private var currOutputs: mutable.Queue[Output] = new mutable.Queue[Output]()
  private var i: Int = 0

  def update(input: I): Unit = {
    i+=1
    if (i % 2 == 0){
      currOutputs.enqueue()
    }
  }

  def outputs(): Iterator[Output] = currOutputs.dequeueAll(_ => true).iterator
}


class StateCreator[I](val outputStrategy: () => OutputStrategy[I],
                      val processors: List[Processor[I]]
                  ){
  def newState: State[I] = {
    val processorPool: ProcessorPool[I] = ProcessorPool.create(processors)
    new State[I](outputStrategy(), processorPool)
  }
}
