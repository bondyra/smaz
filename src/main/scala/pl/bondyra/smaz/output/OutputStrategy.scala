package pl.bondyra.smaz.output

import pl.bondyra.smaz.spark.InputSpec


abstract class OutputStrategy[I](inputSpec: InputSpec[I])


class IntervalOutputStrategy[I](val inputSpec: InputSpec[I], val intervalInMiliseconds: Long)
  extends OutputStrategy[I](inputSpec)
