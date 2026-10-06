import org.apache.spark.sql.SparkSession

object Day11App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Day11-BroadcastAccumulators").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    // Small product reference/master table - broadcast to all executors
    val productPrices: Map[String, Double] = Map("P1" -> 10.0, "P2" -> 20.0, "P3" -> 15.0)
    val broadcastPrices = sc.broadcast(productPrices)

    // Accumulator to count bad records (shared, write-only from tasks, read-only on driver)
    val badRecordCount = sc.longAccumulator("BadRecordCounter")

    // Transactions - some reference products NOT in the master table (bad records)
    val transactions = sc.parallelize(Seq(
      ("T1", "P1", 5),
      ("T2", "P2", 3),
      ("T3", "P9", 2),   // P9 doesn't exist in master table - bad record
      ("T4", "P3", 4),
      ("T5", "PX", 1)    // PX doesn't exist either - bad record
    ))

    // Validate each transaction against the broadcast master table
    val validated = transactions.map { case (txId, product, qty) =>
      broadcastPrices.value.get(product) match {
        case Some(price) => Some((txId, qty * price))
        case None =>
          badRecordCount.add(1)   // accumulator update inside a transformation
          None
      }
    }.filter(_.isDefined).map(_.get)

    println("=== Valid transactions with calculated amounts ===")
    validated.collect().foreach(println)

    // IMPORTANT: accumulator value is only reliable AFTER an action has run
    println(s"\nBad records count: ${badRecordCount.value}")

    spark.stop()
  }
}
