package pl.bondyra.smaz.spark

case class InputSpec[I] private[smaz](keyFunc: I => String, eventTimeFunc: I => String)
