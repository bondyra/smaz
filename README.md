# smaz
This is the place where I will (hopefully) rewrite my old project that had some potential.

Extensible wrapper over Spark Structured Streaming that facilitates custom processing.

Scratch usage (pseudocode):
```
import smaz.Engine

inputData = prepareYourDataset()

engine = new Engine()
.inputData(inputData)
.identifierColumn("input column name")
.eventTimeColumn("input column name")
.sessionTimeout(<how many miliseconds must pass when a new session should start>)
.intervalOutput(<miliseconds of event time when you want a new row out>)

dataStreamWriter = engine.dataStreamWriter()
doSomethingWithYour(dataStreamWriter)
```

TODO:
- determine the safe scope
- write the initial version (it's not a copy-paste unfortunately)
- figure out nested processors
- figure out shared processor state, e.g. if you consider quantiles
- consider other output modes than event time interval (there was a batch marker one and a count based?)

the scope as of Feb 2021:
- slow TDD rewrite
- no YAML or other declarative config
- basic ops without shared state

 
