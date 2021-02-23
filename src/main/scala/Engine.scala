import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row}
case class InputRow(identifier: String, eventTime: Double)

class Engine private(
                      val inputData: Int,
                      val identifierColumn: String,
                      val eventTimeColumn: String,
                      val sessionTimeout: Long,
                      val outputStrategy: OutputStrategy
                    ) {

  implicit val stringEncoder: Encoder[String] = Encoders.kryo[String]
  implicit val stateEncoder: Encoder[State] = Encoders.kryo[State]
  implicit val outputEncoder: Encoder[OutputRow] = Encoders.kryo[OutputRow]


  def doStuff[I <: InputRow](dataset: Dataset[I]): DataFrame = {
    dataset
      .groupByKey(_.identifier)
      .flatMapGroupsWithState[State, OutputRow](
        OutputMode.Append(),
        GroupStateTimeout.NoTimeout() // TODO: the state could have configured expiration time
      )(procFunc)
      .select("*")  // TODO return dataset<outputRow> ? what to type and what not?
  }

  def procFunc(s: String, inputs: Iterator[InputRow], state: GroupState[State]): Iterator[OutputRow] = {
    return Iterator.empty
  }
}

object Engine {
  class Builder {
    private var inputData: Option[Int] = None
    private var idColumn: Option[String] = None
    private var timeColumn: Option[String] = None
    private var sessionTimeout: Option[Long] = None
    private var outputStrategy: Option[OutputStrategy] = None

    def inputData(_inputData: Int): Builder = {
      inputData = Option(_inputData)
      this
    }

    def identifierColumn(idColumnName: String): Builder = {
      idColumn = Option(idColumnName)
      this
    }

    def eventTimeColumn(timeColumnName: String): Builder = {
      timeColumn = Option(timeColumnName)
      this
    }

    def sessionTimeout(sessionTimeoutInMiliseconds: Long): Builder = {
      sessionTimeout = Option(sessionTimeoutInMiliseconds)
      this
    }

    def intervalOutput(intervalLengthInMiliseconds: Long): Builder = {
      outputStrategy = Option(new IntervalOutputStrategy(intervalLengthInMiliseconds))
      this
    }

    def build(): Engine = new Engine(
      inputData=resolveOption(inputData, "Specify the input data set"),
      identifierColumn=resolveOption(idColumn, "Specify name of the column that stores identifiers"),
      eventTimeColumn=resolveOption(timeColumn, "Specify name of the column that stores event times"),
      sessionTimeout=resolveOption(sessionTimeout, "Configure session timeout"),
      outputStrategy=resolveOption(outputStrategy, "Configure output strategy")
    )

    private def resolveOption[A](option: Option[A], errorMessage: String): A =
      option.getOrElse(throw new BuildException(errorMessage))
  }

  class BuildException(message: String) extends Exception

  def builder(): Builder = new Builder
}
