import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row}
case class InputRow(identifier: String, eventTime: Double)

class Engine[I] private(
                       // TODO: if I pass column names here and acquire values with reflection
                       // I will be able to pass the names of columns to output
                      val idFunc: I => String,
                      val eventTimeFunc: I => String,
                      val sessionTimeout: Long,
                      val outputStrategy: OutputStrategy
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
      .select("*")
  }

  def procFunc(s: String, inputs: Iterator[I], groupState: GroupState[State[I]]): Iterator[Output] = {
    val state: State[I] = if (groupState.exists) groupState.get else new State[I]()
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
    private var sessionTimeout: Option[Long] = None
    private var outputStrategy: Option[OutputStrategy] = None

    def idFunc(_idFunc: I => String): Builder[I] = {
      idFunc = Option(_idFunc)
      this
    }

    def eventTimeFunc(_eventTimeFunc: I => String): Builder[I] = {
      eventTimeFunc = Option(_eventTimeFunc)
      this
    }

    def sessionTimeout(sessionTimeoutInMiliseconds: Long): Builder[I] = {
      sessionTimeout = Option(sessionTimeoutInMiliseconds)
      this
    }

    def intervalOutput(intervalLengthInMiliseconds: Long): Builder[I] = {
      outputStrategy = Option(new IntervalOutputStrategy(intervalLengthInMiliseconds))
      this
    }

    def build(): Engine[I] = new Engine[I](
      idFunc=resolveOption(idFunc, "Specify mapping to id column"),
      eventTimeFunc=resolveOption(eventTimeFunc, "Specify mapping to event time column"),
      sessionTimeout=resolveOption(sessionTimeout, "Configure session timeout"),
      outputStrategy=resolveOption(outputStrategy, "Configure output strategy")
    )

    private def resolveOption[A](option: Option[A], errorMessage: String): A =
      option.getOrElse(throw new BuildException(errorMessage))
  }

  class BuildException(message: String) extends Exception

  def builder[I](): Builder[I] = new Builder[I]
}
