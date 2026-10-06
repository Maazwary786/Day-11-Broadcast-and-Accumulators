# Day 11 — Broadcast and Accumulators

## Task
Broadcast a small product reference map, use an accumulator to count bad records, and validate transactions against a small master table.

## Code — `src/main/scala/Day11App.scala`
```scala
import org.apache.spark.sql.SparkSession

object Day11App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Day11-BroadcastAccumulators").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    val productPrices: Map[String, Double] = Map("P1" -> 10.0, "P2" -> 20.0, "P3" -> 15.0)
    val broadcastPrices = sc.broadcast(productPrices)

    val badRecordCount = sc.longAccumulator("BadRecordCounter")

    val transactions = sc.parallelize(Seq(
      ("T1", "P1", 5),
      ("T2", "P2", 3),
      ("T3", "P9", 2),
      ("T4", "P3", 4),
      ("T5", "PX", 1)
    ))

    val validated = transactions.map { case (txId, product, qty) =>
      broadcastPrices.value.get(product) match {
        case Some(price) => Some((txId, qty * price))
        case None =>
          badRecordCount.add(1)
          None
      }
    }.filter(_.isDefined).map(_.get)

    println("=== Valid transactions with calculated amounts ===")
    validated.collect().foreach(println)

    println(s"\nBad records count: ${badRecordCount.value}")

    spark.stop()
  }
}
```

## Output
=== Valid transactions with calculated amounts ===
(T1,50.0)
(T2,60.0)
(T4,60.0)

Bad records count: 2
## Explanation — what's happening

**1. Broadcast variable**
```scala
val broadcastPrices = sc.broadcast(productPrices)
```
`productPrices` is small (3 entries) but every task across every executor needs to read it. Without broadcasting, Spark would serialize and ship a fresh copy of this map with *every single task* that closes over it. `sc.broadcast(...)` instead sends it to each executor **once**, cached there, and every task on that executor shares the same read-only copy via `.value`.

**2. Accumulator**
```scala
val badRecordCount = sc.longAccumulator("BadRecordCounter")
...
badRecordCount.add(1)
```
An accumulator is a special shared variable that tasks can only **add to**, never read reliably mid-job — only the driver can read its final value, and only after an action has completed. It's built to survive Spark's retry/speculative-execution model safely for counters like this.

**3. Why NOT a normal driver `var`?**
If you tried `var badCount = 0` and incremented it inside `transactions.map { ... badCount += 1 ... }`, it would silently give wrong results. Each task runs in a **separate executor JVM**, Spark serializes the closure (including `badCount`) and sends a *copy* to each task — incrementing that copy never updates the original variable back on the driver. Accumulators are specifically engineered by Spark to correctly aggregate updates from all tasks back to the driver; plain closures over driver variables are not.

**4. Validation logic**
```scala
broadcastPrices.value.get(product) match {
  case Some(price) => Some((txId, qty * price))
  case None => badRecordCount.add(1); None
}
```
For each transaction, look up the product in the broadcast map. If found, compute `qty * price`. If not found (`P9`, `PX`), increment the accumulator and produce `None`, which gets filtered out afterward.

**5. Accumulator timing**
`badRecordCount.value` is only printed *after* `validated.collect()` has run — accumulator values aren't guaranteed accurate until the action that updates them has actually executed.

## Viva Q&A
| Question | Answer |
|---|---|
| Why broadcast the product map instead of just referencing it in the closure? | Without broadcasting, Spark ships a fresh serialized copy of the map with every task; broadcasting sends it once per executor and all tasks on that executor share the cached copy — much less network/memory overhead for data reused across many tasks. |
| Why can't you use a plain driver `var` to count bad records? | Each task runs in a separate executor JVM on a serialized *copy* of the closure; incrementing that copy never propagates back to the driver's original variable — accumulators are purpose-built by Spark to correctly merge per-task updates back to the driver. |
| When is it safe to read an accumulator's value? | Only on the driver, and only after the action that triggers the accumulator updates has completed — reading it mid-transformation or before an action runs isn't reliable. |
| What would happen if P9 and PX were both valid products? | `badRecordCount` would be 0, and all 5 transactions would appear in the valid output. |
| Is `broadcastPrices.value` read-only? | Yes — broadcast variables are meant to be read-only on executors; they're not a mechanism for executors to write data back. |
