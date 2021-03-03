package pl.bondyra.smaz.output

import pl.bondyra.smaz.input.Input

import scala.collection.mutable

abstract class Combiner[I <: Input](val outputStrategy: OutputStrategy[I]) {
  def doWork(input: I, processorValues: Map[String, String]): Unit
  def drainOutputs(): Iterator[Output]
}


class SimpleCombiner[I <: Input](outputStrategy: OutputStrategy[I]) extends Combiner[I](outputStrategy) {
  private var currentVersion = 0
  private val currentOutputs: mutable.Queue[Output] = new mutable.Queue[Output]()

  override def doWork(input: I, processorValues: Map[String, String]): Unit = {
    if (outputStrategy.canDo(input)){
      currentOutputs.enqueue(Output(input.identifier, input.eventTime, currentVersion, processorValues))
      currentVersion += 1
    }
  }

  override def drainOutputs(): Iterator[Output] = currentOutputs.dequeueAll(_ => true).iterator
}
