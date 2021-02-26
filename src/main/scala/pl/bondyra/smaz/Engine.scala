package pl.bondyra.smaz


import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders}
import pl.bondyra.smaz.output.{IntervalOutputStrategy, Output}
import pl.bondyra.smaz.state.{State}
case class InputRow(identifier: String, eventTime: Double)

class Engine[I] private(
                      val idFunc: I => String,
                      val eventTimeFunc: I => String,
                      val stateCreator: () => State[I]
                    ) {

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
      .select(Output.columnsToSelect: _*)
  }

  def procFunc(s: String, inputs: Iterator[I], groupState: GroupState[State[I]]): Iterator[Output] = {
    val state: State[I] = if (groupState.exists) groupState.get else stateCreator()
    for (input <- inputs){
      state.update(input)
    }
    groupState.update(state)
    state.outputs()
  }
}

object Engine {
  class Builder[I] {
    private var idFunc: Option[I => String] = None
    private var eventTimeFunc: Option[I => String] = None
    private var stateCreator: Option[() => State[I]] = None

    def idFunc(_idFunc: I => String): Builder[I] = {
      idFunc = Option(_idFunc)
      this
    }

    def eventTimeFunc(_eventTimeFunc: I => String): Builder[I] = {
      eventTimeFunc = Option(_eventTimeFunc)
      this
    }

    def intervalOutput(intervalInMiliseconds: Long): Builder[I] = {
      stateCreator = Option(() => new State[I](new IntervalOutputStrategy(intervalInMiliseconds)))
      this
    }

    def build(): Engine[I] = new Engine[I](
      idFunc=resolveOption(idFunc),
      eventTimeFunc=resolveOption(eventTimeFunc),
      stateCreator=resolveOption(stateCreator)
    )

    private def resolveOption[A](option: Option[A]): A = option.getOrElse(throw new BuildException())
  }

  class BuildException() extends Exception

  def builder[I](): Builder[I] = new Builder[I]
}
