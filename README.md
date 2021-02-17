# smaz
This is the place where I will (hopefully) rewrite my old project that had some potential.

Extensible wrapper over Spark Structured Streaming that facilitates custom processing.

Scratch usage (pseudocode):
```
import smaz.Engine

inputData = prepareYourDataset()

engine = new Engine()
.input(inputData)
.spec(<config file that declares supported processors>)
.key("input column name")
.eventTime("input column name")
.sessionTimeout(<how many miliseconds must pass when a new session should start>)
.output(INTERVAL, <miliseconds of event time when you want a new row out>)

dataStreamWriter = engine.dataStreamWriter()
doSomethingWithYour(dataStreamWriter)
```

Config file could be a YAML:
```
- proc1
    class: omg.bondyra.smaz.processors.Sum
    column: SUM_OF_STUFF
    args:
        input_column:
            type: string
            value: SOME_INPUT_COLUMN
```
You will get an output periodically sends a sum of values in SOME_INPUT_COLUMN.


TODO:
- determine the safe scope
- write the initial version (it's not a copy-paste unfortunately)
- figure out nested processors
- figure out shared processor state, e.g. if you consider quantiles
- consider other output modes than event time interval (there was a batch marker one and a count based?)


 
