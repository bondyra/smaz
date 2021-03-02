package pl.bondyra.smaz.processor

abstract class Processor[I](val name: String) {
  def duplicate(): Processor[I]
  def reset(): Unit
  def update(row: I): Unit
}


class ColumnIntSumProcessor[I](name: String, getAggregatedValue: I => Int) extends Processor[I](name){
  var currentSum: Int = 0
  override def duplicate(): Processor[I] = new ColumnIntSumProcessor[I](name, getAggregatedValue)

  override def reset(): Unit = {
    currentSum = 0
  }

  override def update(row: I): Unit = {
    currentSum += getAggregatedValue(row)
  }
}
