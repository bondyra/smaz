package pl.bondyra.smaz.spark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Encoders}
import pl.bondyra.smaz.output.Output
import pl.bondyra.smaz.state.{State, StateSpec}
case class InputRow(identifier: String, eventTime: Double)

class Engine[I] private(val stateSpec: StateSpec[I]) {

  implicit val stringEncoder: Encoder[String] = Encoders.kryo[String]
  implicit val stateEncoder: Encoder[State[I]] = Encoders.kryo[State[I]]
  implicit val outputEncoder: Encoder[Output] = Encoders.kryo[Output]


  def run(dataset: Dataset[I]): DataFrame = {
    dataset
      .groupByKey(stateSpec.idFunc)
      .flatMapGroupsWithState[State[I], Output](
        OutputMode.Append(),
        GroupStateTimeout.NoTimeout()
      )(procFunc)
      .select(columnsToSelect: _*)
  }

  def procFunc(s: String, inputs: Iterator[I], groupState: GroupState[State[I]]): Iterator[Output] = {
    val state: State[I] = if (groupState.exists) groupState.get else stateSpec.newState
    for (input <- inputs){
      state.update(input)
    }
    groupState.update(state)
    state.outputs()
  }

  private def columnsToSelect: Seq[Column] = stateSpec.processorNames.map(col)
}
