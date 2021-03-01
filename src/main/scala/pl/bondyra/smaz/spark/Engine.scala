package pl.bondyra.smaz.spark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Encoders}
import pl.bondyra.smaz.output.{IntervalOutputStrategy, Output, OutputStrategy}
import pl.bondyra.smaz.processor.Processor
import pl.bondyra.smaz.state.{State, StateCreator}

case class InputRow(identifier: String, eventTime: Double)

class Engine[I] private(val idFunc: I => String, val stateCreator: StateCreator[I]) {
  implicit val stringEncoder: Encoder[String] = Encoders.kryo[String]
  implicit val stateEncoder: Encoder[State[I]] = Encoders.kryo[State[I]]
  implicit val outputEncoder: Encoder[Output] = Encoders.kryo[Output]


  def run(dataset: Dataset[I]): DataFrame = {
    dataset
      .groupByKey(idFunc)
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

  class Builder[I] {
    private var processors: List[Processor] = List.empty
    private var idFunc: Option[I => String] = None
    private var eventTimeFunc: Option[I => String] = None
    private var outputStrategyCreator: Option[() => OutputStrategy] = None

    def idFunc(_idFunc: I => String): Builder[I] = {
      idFunc = Option(_idFunc)
      this
    }

    // TODO this must precede all lower functions!
    def eventTimeFunc(_eventTimeFunc: I => String): Builder[I] = {
      eventTimeFunc = Option(_eventTimeFunc)
      this
    }

    def intervalOutput(intervalInMiliseconds: Long): Builder[I] = {
      outputStrategyCreator = Option(() => new IntervalOutputStrategy(intervalInMiliseconds))
      this
    }

    def withProcessor(processor: Processor): Builder[I] = {
      processors ::= processor
      this
    }

    def build(): Engine[I] = {

      // eventTimeFunc=resolveOption(eventTimeFunc)

      val stateCreator = new StateCreator[I](resolveOption(outputStrategyCreator), processors)

      new Engine[I](
        idFunc = resolveOption(idFunc),
        stateCreator = stateCreator
      )
    }

    private def resolveOption[A](option: Option[A]): A = option.getOrElse(throw new BuildException())

    class BuildException() extends Exception

    def builder[I](): Builder[I] = new Builder[I]
  }

}
