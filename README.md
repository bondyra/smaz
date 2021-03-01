# smaz
This is the place where I will (hopefully) rewrite my old project that had some potential.

Extensible wrapper over Spark Structured Streaming that facilitates custom processing.


TODO:
- determine the safe scope
- write the initial version (it's not a copy-paste unfortunately)
- figure out nested processors
- figure out shared processor state, e.g. if you consider quantiles
- consider other pl.bondyra.smaz.output modes than event time interval (there was a batch marker one and a count based?)

the scope as of Feb 2021:
- slow TDD rewrite
- no YAML or other declarative config
- basic ops without shared state

![state high level design](doc/smaz0.png?raw=true)
