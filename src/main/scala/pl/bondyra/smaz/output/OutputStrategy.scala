package pl.bondyra.smaz.output

import pl.bondyra.smaz.input.Input


trait OutputStrategy[I <: Input] {
  def canDo(input: I): Boolean
}


class IntervalOutputStrategy[I <: Input](val intervalInMiliseconds: Double) extends OutputStrategy[I] {
  private var markedEventTime: Double = 0d

  def canDo(input: I): Boolean = {
    if (input.eventTime >= markedEventTime + intervalInMiliseconds){
      markedEventTime = input.eventTime
      return true
    }
    false
  }
}
