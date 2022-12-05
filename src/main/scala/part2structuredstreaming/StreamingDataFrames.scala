package part2structuredstreaming

import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import common._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object StreamingDataFrames {

  val spark = SparkSession.builder()
    .appName("Streaming")
    .config("spark.master", "local[2]")
    .getOrCreate()

  def readFromSocket() = {
    // Reading a DF
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // transformation
    val shortLines = lines.filter(length(col("value") <= 5))
    println(shortLines.isStreaming) // tell between a static and a streaming df

    val query = lines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    // need to wait for stream to finish
    query.awaitTermination()
  }

  def readFromFiles() = {
    val stocksDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("dateFormat", "MMM d YYYY")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  def demoTriggers() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // write the lines of the DF at a certain trigger
    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds)) // check every 2 seconds for new data in DF
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    demoTriggers()
  }

}
