

import java.io.{File, FileFilter}
import java.nio.charset.Charset
import java.util.UUID.randomUUID

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import pl.bondyra.smaz.input.Input
import pl.bondyra.smaz.processor.IntSumProcessor
import pl.bondyra.smaz.spark.Engine

import scala.collection.mutable

case class ExampleInput(id: String, timestamp: Long, value: Int) extends Input {
  override def identifier: String = id

  override def eventTime: Long = timestamp
}

class MainTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {
  var inputDirPath: String = _
  var checkpointDirPath: String = _
  var outputDirPath: String = _

  var sparkSession: SparkSession = _
  var runId: String = _

  override def beforeAll() {
    sparkSession = SparkSession.builder()
      .appName("integration-tests")
      .master("local[4]")
      .getOrCreate()
  }

  override def beforeEach() {
    runId = randomUUID().toString
    inputDirPath = s"test-input-$runId"
    checkpointDirPath = s"test-checkpoint-$runId"
    outputDirPath = s"test-output-$runId"
  }

  override def afterEach() {
    FileUtils.deleteDirectory(new File(inputDirPath))
    FileUtils.deleteDirectory(new File(checkpointDirPath))
    FileUtils.deleteDirectory(new File(outputDirPath))
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
      ExampleInput("b", 3, 1)
    )
    sparkSession
      .readStream
      .option("header", "false")
      .schema(schema)
      .csv(inputDirPath)
      .as(Encoders.product[ExampleInput])
  }

  test("test") {
    Engine.builder
      .withProcessor(new IntSumProcessor[ExampleInput]("MY_SUM", _.value))
      .intervalOutput(1)
      .build()
      .run(inputDataset)
      .writeStream
      .format("csv")
      .option("header", "true")
      .option("checkpointLocation", checkpointDirPath)
      .outputMode("append")
      .start(outputDirPath)
      .awaitTermination(8 * 1000)

    val header: String = "identifier,eventTime,version,MY_SUM"
    checkOutput(
      ("a", s"$header|a,2,1,2|a,3,2,3".replace("|", "\n")),
      ("b", s"$header|b,2,1,2|b,3,2,3".replace("|", "\n"))
    )
  }

  def checkOutput(keysAndContents: (String, String)*): Unit = {
    val fileContents = getAllNonEmptySparkFileContents(outputDirPath)
    assert(fileContents.length == keysAndContents.size)
    val alreadyAssertedGroups = mutable.HashSet[String]()
    for ((key, content) <- keysAndContents) {
      for (fileContent <- fileContents) {
        if (fileContent.contains(key)) {
          assert(!alreadyAssertedGroups.contains(key))
          assert(fileContent == content)
          alreadyAssertedGroups.add(key)
        }
      }
    }
  }

  def getAllNonEmptySparkFileContents(dirPath: String): Array[String] = {
    val filter: FileFilter = new WildcardFileFilter("part-*.csv")
    new File(dirPath)
      .listFiles(filter)
      .filter(f => f.length > 0)
      .map(f => FileUtils.readFileToString(f, Charset.defaultCharset()))
  }
}
