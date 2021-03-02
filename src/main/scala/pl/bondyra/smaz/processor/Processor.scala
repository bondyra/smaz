package pl.bondyra.smaz.processor

abstract class Processor(name: String) {
  def duplicate(): Processor
}


class ExampleProcessor(val name: String, var someValue: Int = 0) extends Processor(name){
  override def duplicate(): Processor = {
    new ExampleProcessor(name, 0)
  }
}
