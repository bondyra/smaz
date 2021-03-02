package pl.bondyra.smaz.spark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Encoders}
import pl.bondyra.smaz.output.{IntervalOutputStrategy, Output, OutputStrategy}
import pl.bondyra.smaz.processor.Processor
import pl.bondyra.smaz.state.{State, StateCreator}

case class InputRow(identifier: String, eventTime: Double)

class Engine[I] private(val inputSpec: InputSpec[I], val stateCreator: StateCreator[I]) {
  implicit val stringEncoder: Encoder[String] = Encoders.kryo[String]
  implicit val stateEncoder: Encoder[State[I]] = Encoders.kryo[State[I]]
  implicit val outputEncoder: Encoder[Output] = Encoders.kryo[Output]


  def run(dataset: Dataset[I]): DataFrame = {
    dataset
      .groupByKey(inputSpec.keyFunc)
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

  class Builder[I](val inputSpec: InputSpec[I]) {
    private var processors: List[Processor[I]] = List.empty
    private var outputStrategyCreator: Option[() => OutputStrategy[I]] = None

    def intervalOutput(intervalInMiliseconds: Long): Builder[I] = {
      outputStrategyCreator = Option(() => new IntervalOutputStrategy[I](inputSpec, intervalInMiliseconds))
      this
    }

    def withProcessor(processor: Processor[I]): Builder[I] = {
      processors ::= processor
      this
    }

    def build(): Engine[I] = {
      val stateCreator = new StateCreator[I](resolveOption(outputStrategyCreator), processors)

      new Engine[I](
        inputSpec = inputSpec,
        stateCreator = stateCreator
      )
    }

    private def resolveOption[A](option: Option[A]): A = option.getOrElse(throw new BuildException())

    class BuildException() extends Exception

    def builder(inputSpec: InputSpec[I]): Builder[I] = new Builder[I](inputSpec)
  }
}
