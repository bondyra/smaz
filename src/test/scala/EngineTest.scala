import org.scalatest.FunSuite
import pl.bondyra.smaz.input.Input
import pl.bondyra.smaz.processor.IntSumProcessor
import pl.bondyra.smaz.spark.Engine
import pl.bondyra.smaz.spark.Engine.BuildException

class EngineTest extends FunSuite {
  private case class ExampleInput(value: Int) extends Input{
    override def identifier: String = "dummy"
    override def eventTime: Long = 10
  }

  test("User cannot create engine without processors"){
    assertThrows[BuildException](
      Engine.builder
        .intervalOutput(1)
        .build()
    )
  }

  test("User cannot create engine without configured output algorithm"){
    assertThrows[BuildException](
      Engine.builder[ExampleInput]
          .withProcessor(new IntSumProcessor[ExampleInput]("name", _.value))
        .build()
    )
  }

  test("User can create engine with required config and can see this config"){
    Engine.builder
      .intervalOutput(1)
      .withProcessor(new IntSumProcessor[ExampleInput]("name", _.value))
      .build()
  }
}
