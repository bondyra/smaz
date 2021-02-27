package pl.bondyra.smaz.state
import pl.bondyra.smaz.config.{Config, IntervalOutputType}
import pl.bondyra.smaz.output.{IntervalOutputStrategy, Output, OutputStrategy}
import pl.bondyra.smaz.processor.ProcessorPool

import scala.collection.mutable

class State[I] (val outputStrategy: OutputStrategy, val processorPool: ProcessorPool) {
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


class StateSpec[I](val config: Config){
  private def fieldValue(c: I, field: String) = {
    c.getClass.getDeclaredField(field).get(c)
  }

  def idFunc: I => String = fieldValue(_, config.idColumnName).asInstanceOf[String]

  def eventTimeFunc: I => Long = fieldValue(_, config.eventTimeColumnName).asInstanceOf[Long]

  def processorNames: List[String] = config.processorDefinitions.map(_.name)

  def newState: State[I] = config.outputType match {
    case IntervalOutputType(time: Long) => new State[I](
      new IntervalOutputStrategy(time), ProcessorPool.create(config.processorDefinitions))
    case _ => throw new Exception("Not implemented")
  }
}
