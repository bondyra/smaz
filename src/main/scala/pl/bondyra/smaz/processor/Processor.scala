package pl.bondyra.smaz.processor

abstract class Processor(val name: String) {
  def duplicate(): Processor
}
