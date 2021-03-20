import java.io.File
import java.nio.charset.Charset
import java.util.UUID.randomUUID

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite, Matchers}
import pl.bondyra.smaz.input.Input
import pl.bondyra.smaz.processor.IntSumProcessor
import pl.bondyra.smaz.spark.Engine

case class ExampleInput(id: String, timestamp: Long, value: Int) extends Input {
  override def identifier: String = id

  override def eventTime: Long = timestamp
}

case class Result(identifier: String, eventTime: Long, version: Long, MY_SUM: String)

object implicits {
  implicit val inputEncoder: Encoder[ExampleInput] = Encoders.product[ExampleInput]
  implicit val resultEncoder: Encoder[Result] = Encoders.product[Result]
}

class MainTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach with Matchers {
  var inputDirPath: String = _
  var checkpointDirPath: String = _

  var sparkSession: SparkSession = _
  var runId: String = _

  override def beforeAll() {
    sparkSession = SparkSession.builder()
      .appName("integration-tests")
      .master("local[4]")
      .getOrCreate()
  }

  override def beforeEach() {
    runId = randomUUID().toString.replace("-", "")
    inputDirPath = s"test-input-$runId"
    checkpointDirPath = s"test-checkpoint-$runId"
  }

  override def afterEach() {
    FileUtils.deleteDirectory(new File(inputDirPath))
    FileUtils.deleteDirectory(new File(checkpointDirPath))
  }

  def prepareData(inputs: ExampleInput*): Unit = {
    assert(new File(inputDirPath).mkdir)
    val file: File = new File(inputDirPath, "file.csv")
    file.createNewFile()
    val content: String = inputs
      .map(input => s"${input.id},${input.timestamp},${input.value}")
      .mkString("\n")
    FileUtils.writeStringToFile(file, content, Charset.defaultCharset())
  }

  def schema: StructType = StructType(
    StructField("id", DataTypes.StringType) ::
      StructField("timestamp", DataTypes.LongType) ::
      StructField("value", DataTypes.IntegerType) :: Nil
  )

  def inputDataset: Dataset[ExampleInput] = {
    prepareData(
      ExampleInput("a", 1, 1),
      ExampleInput("b", 1, 1),
      ExampleInput("a", 2, 1),
      ExampleInput("b", 2, 1),
      ExampleInput("a", 3, 1),
      ExampleInput("b", 3, 1),
    )
    sparkSession
      .readStream
      .option("header", "false")
      .schema(schema)
      .csv(inputDirPath)
      .as(Encoders.product[ExampleInput])
  }

  test("test") {
    import implicits._
    Engine.builder
      .withProcessor(new IntSumProcessor[ExampleInput]("MY_SUM", _.value))
      .intervalOutput(1)
      .build()
      .run(inputDataset)
      .writeStream
      .queryName(runId)
      .option("checkpointLocation", checkpointDirPath)
      .outputMode("append")
      .format("memory")
      .start()
      .awaitTermination(4 * 1000)

    val actualResults: Array[Result] = sparkSession.sql(s"select * from $runId").as[Result].collect()
    val expectedResults: Array[Result] = Array(
        Result(identifier = "a", eventTime = 1, version = 0, MY_SUM = "1"),
        Result(identifier = "a", eventTime = 2, version = 1, MY_SUM = "2"),
        Result(identifier = "a", eventTime = 3, version = 2, MY_SUM = "3"),
        Result(identifier = "b", eventTime = 1, version = 0, MY_SUM = "1"),
        Result(identifier = "b", eventTime = 2, version = 1, MY_SUM = "2"),
        Result(identifier = "b", eventTime = 3, version = 2, MY_SUM = "3")
      )
    actualResults should contain theSameElementsAs expectedResults
  }
}
