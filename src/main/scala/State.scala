import org.apache.spark.sql.Row

import scala.collection.mutable

class State[I] {
  private var currOutputs: mutable.Queue[Output] = new mutable.Queue[Output]()
  private var i: Int = 0

  def update(input: I) = {
    i+=1
    if (i % 2 == 0){
      currOutputs.enqueue(new Output())
    }
  }

  def outputs(): Iterator[Output] = currOutputs.dequeueAll(_ => true).iterator
}
