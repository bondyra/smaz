package pl.bondyra.smaz.state
import pl.bondyra.smaz.output.{IntervalOutputStrategy, Output, OutputStrategy}

import scala.collection.mutable

class State[I] (val outputStrategy: OutputStrategy) {
  private var currOutputs: mutable.Queue[Output] = new mutable.Queue[Output]()
  private var i: Int = 0

  def update(input: I) = {
    i+=1
    if (i % 2 == 0){
      currOutputs.enqueue()
    }
  }

  def outputs(): Iterator[Output] = currOutputs.dequeueAll(_ => true).iterator
}
