package part5advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, window}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import java.io.PrintStream
import java.net.ServerSocket
import scala.concurrent.duration._
import java.sql.Timestamp

object Watermarks {

  val spark = SparkSession.builder()
    .appName("Watermarks")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  def debugQuery(query: StreamingQuery) = {
    new Thread(() => {
      (1 to 100).foreach { i =>
        Thread.sleep(1000)
        val queryEventTime =
          if (query.lastProgress == null) "[]"
          else query.lastProgress.eventTime.toString

        println(s"$i: $queryEventTime")
      }
    }).start()
  }

  def testWatermark() = {
    val dataDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map {
        line =>
          val tokens = line.split(",")
          val timestamp = new Timestamp(tokens(0).toLong)
          val data = tokens(1)
          (timestamp, data)
      }.toDF("created", "color")

    val watermarkedDF = dataDF
      .withWatermark("created", "2 seconds")
      .groupBy(window(col("created"), "2 seconds"), col("color"))
      .count()
      .selectExpr("window.*", "color", "count")

    /* A 2 second watermark means:
        - a window will only be considered until the watermark surpasses the window end
        - an element/row/record will be considered if after the watermark
     */

    val query = watermarkedDF.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()

    // useful for debugging
    debugQuery(query)
    query.awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    testWatermark()
  }

}


object DataSender {
  val serverSocket = new ServerSocket(12345)
  val socket = serverSocket.accept() // blocking call
  val printer = new PrintStream(socket.getOutputStream)

  println("socket accepted")

  def example1() = {
    Thread.sleep(7000)
    printer.println("7000, blue")
    Thread.sleep(1000)
    printer.println("8000, green")
    Thread.sleep(4000)
    printer.println("12000, red")
    Thread.sleep(5000)
    printer.println("17000, gold")
    printer.println("17000, green")
    Thread.sleep(2000)
    printer.println("21000, green")
    Thread.sleep(4000)
    printer.println("21000, green")
  }

  def main(args: Array[String]): Unit = {
    example1()
  }
}