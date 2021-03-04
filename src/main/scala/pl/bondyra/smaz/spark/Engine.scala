package pl.bondyra.smaz.spark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Encoders}
import pl.bondyra.smaz.input.Input
import pl.bondyra.smaz.output.{Combiner, IntervalOutputStrategy, Output, OutputStrategy, SimpleCombiner}
import pl.bondyra.smaz.processor.Processor
import pl.bondyra.smaz.state.{State, StateCreator}


class Engine[I <: Input] private(val stateCreator: StateCreator[I]) {
  implicit val stringEncoder: Encoder[String] = Encoders.kryo[String]
  implicit val stateEncoder: Encoder[State[I]] = Encoders.kryo[State[I]]
  implicit val outputEncoder: Encoder[Output] = Encoders.kryo[Output]


  def run(dataset: Dataset[I]): DataFrame = {
    dataset
      .groupByKey(_.identifier)
      .flatMapGroupsWithState[State[I], Output](
      OutputMode.Append(),
      GroupStateTimeout.NoTimeout()
    )(procFunc)
      .select(columnsToSelect: _*)
  }

  def procFunc(s: String, inputs: Iterator[I], groupState: GroupState[State[I]]): Iterator[Output] = {
    val state: State[I] = if (groupState.exists) groupState.get else stateCreator.newState
    for (input <- inputs) {
      state.update(input)
    }
    groupState.update(state)
    state.outputs()
  }

  private def columnsToSelect: Seq[Column] = stateCreator.processors.map(p => col(p.name))
}


object Engine {

  class Builder[I <: Input] {
    private var processors: List[Processor[I]] = List.empty
    private var createCombiner: Option[() => Combiner[I]] = None

    def intervalOutput(intervalInMiliseconds: Long): Builder[I] = {
      createCombiner = Option(() => new SimpleCombiner[I](new IntervalOutputStrategy[I](intervalInMiliseconds)))
      this
    }

    def withProcessor(processor: Processor[I]): Builder[I] = {
      processors ::= processor
      this
    }

    def build(): Engine[I] = {
      if (processors.isEmpty)
        throw new BuildException()
      val stateCreator = new StateCreator[I](resolveOption(createCombiner), processors)

      new Engine[I](
        stateCreator = stateCreator
      )
    }

    private def resolveOption[A](option: Option[A]): A = option.getOrElse(throw new BuildException())
  }

  class BuildException() extends Exception

  def builder[I <: Input]: Builder[I] = new Builder[I]
}
