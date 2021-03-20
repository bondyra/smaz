package pl.bondyra.smaz.processor

import pl.bondyra.smaz.input.Input

abstract class Processor[I <: Input](val name: String) extends java.io.Serializable {
  def duplicate(): Processor[I]
  def reset(): Unit
  def update(row: I): Unit
  def stringValue: String
}


class IntSumProcessor[I <: Input](name: String, getValue: I => Int) extends Processor[I](name){
  var currentSum: Int = 0
  override def duplicate(): Processor[I] = new IntSumProcessor[I](name, getValue)

  override def reset(): Unit = {
    currentSum = 0
  }

  override def update(row: I): Unit = {
    currentSum += getValue(row)
  }

  override def stringValue: String = currentSum.toString
}
