# smaz
This is the place where I will (hopefully) rewrite my old project that had some potential.

Extensible wrapper over Spark Structured Streaming that facilitates custom processing.

Scratch usage (pseudocode):
```
input_data = make_some_spark_sql_operations_to_prepare_your_dataset()

data_stream_writer = smaz
.input(input_data)
.spec(<config file that declares supported processors>)
.key("input column name")
.event_time("input column name")
.timeout 
.output(INTERVAL, <miliseconds of event time when you want a new row out>)

do_something_with_your(data_stream_writer)
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
- write the initial version (it's not a copy-paste unfortunately)
- make the interface extensible
- figure out nested processors
- figure out shared processor state, e.g. if you consider quantiles
- make yaml more concise
- consider other output modes than event time interval (there was a batch marker one and a count based?)
 