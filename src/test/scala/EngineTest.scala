import Engine.BuildException
import org.scalatest.FunSuite

class EngineTest extends FunSuite {
  test("User cannot create engine without configured input dataset"){
    assertThrows[BuildException](
      Engine.builder()
        .identifierColumn("a")
        .eventTimeColumn("a")
        .intervalOutput(1)
        .sessionTimeout(1)
        .build()
    )
  }

  test("User cannot create engine without configured identifier column"){
    assertThrows[BuildException](
      Engine.builder()
        .inputData(1)
        .eventTimeColumn("a")
        .intervalOutput(1)
        .sessionTimeout(1)
        .build()
    )
  }

  test("User cannot create engine without configured event time column"){
    assertThrows[BuildException](
      Engine.builder()
        .inputData(1)
        .identifierColumn("ab")
        .intervalOutput(1)
        .sessionTimeout(1)
        .build()
    )
  }

  test("User cannot create engine without configured output mode"){
    assertThrows[BuildException](
      Engine.builder()
        .inputData(1)
        .identifierColumn("a")
        .eventTimeColumn("a")
        .sessionTimeout(1)
        .build()
    )
  }

  test("User cannot create engine without configured session timeout"){
    assertThrows[BuildException](
      Engine.builder()
        .inputData(1)
        .identifierColumn("a")
        .eventTimeColumn("a")
        .intervalOutput(1)
        .build()
    )
  }

  test("User can create engine with required config and can see this config"){
    val engine: Engine = Engine.builder()
      .inputData(1)
      .identifierColumn("ic")
      .eventTimeColumn("etc")
      .intervalOutput(1)
      .sessionTimeout(1)
      .build()

    assert(engine.inputData == 1)
    assert(engine.identifierColumn == "ic")
    assert(engine.eventTimeColumn == "etc")
    assert(engine.outputStrategy.isInstanceOf[IntervalOutputStrategy])
    assert(engine.sessionTimeout == 1)
  }
}
