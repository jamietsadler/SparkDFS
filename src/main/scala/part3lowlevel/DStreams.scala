package part3lowlevel

import java.sql.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import common._

import java.io.{File, FileWriter}

object DStreams {
  val spark = SparkSession.builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1)) // spark streaming context, entry point to DStreams API
  // poll for new data every second

  // cant restart computation once its been stopped

  def readFromSocket() = {
    val socketStream: DStream[String] = ssc.socketTextStream("localhost", 12345)

    // transformation - lazy
    val wordsStream: DStream[String] = socketStream.flatMap(line => line.split(" "))

    // action
    //wordsStream.print()

    wordsStream.saveAsTextFiles("src/main/resources/data/words")

    ssc.start()
    ssc.awaitTermination()
  }

  def createNewFile() = {
    new Thread(() => {
      Thread.sleep(5000)

      val path = "src/main/resources/data/stocks"
      val dir = new File(path)
      val nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStocks$nFiles.csv")
      newFile.createNewFile()

      val writer = new FileWriter(newFile)
      writer.write(
        "AAPL,Jan 1 2000,25.94\nAAPL,Feb 1 2000,28.66\nAAPL,Mar 1 2000,33.95\nAAPL,Apr 1 2000,31.01\nAAPL,May 1 2000,21\nAAPL,Jun 1 2000,26.19\nAAPL,Jul 1 2000,25.41\nAAPL,Aug 1 2000,30.47\nAAPL,Sep 1 2000,12.88\nAAPL,Oct 1 2000,9.78\nAAPL,Nov 1 2000,8.25\nAAPL,Dec 1 2000,7.44"
      .stripMargin.trim)

      writer.close()
    }).start()

  }

  def readFromFile() = {
    createNewFile()
    // defined DStream
    val stocksFilePath: String = "src/main/resources/data/stocks"
    val textStream = ssc.textFileStream(stocksFilePath)

    // transformations
    val dateFormat = new SimpleDateFormat("MMM d yyy")

    val stocksSteam = textStream.map {
      line =>
        val tokens = line.split(" ")
        val company = tokens(0)
        val date = new Date(dateFormat.parse(tokens(1)).getTime)
        val price = tokens(2).toDouble

        Stock(company, date, price)
    }

    // action
    stocksSteam.print()

    // start computation
    ssc.start()
    ssc.awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    readFromSocket()
  }

}
