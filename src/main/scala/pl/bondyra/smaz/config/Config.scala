package pl.bondyra.smaz.config

class Config[I](
            val idFunc: I => String,
            val eventTimeFunc: I => String
            )
