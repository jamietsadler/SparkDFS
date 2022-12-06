package part3lowlevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import common._
import org.apache.spark.streaming.dstream.DStream

import java.sql.Date
import java.time.{LocalDate, Period}
import java.io.{File, FileWriter}

object DStreamsTransformations {

  val spark = SparkSession.builder()
    .appName("DStream Transformations")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

  def readPeople() = ssc.socketTextStream("localhost", 9999).map { line =>
    val tokens  = line.split(":")
    Person(
      tokens(0).toInt, // id
      tokens(1), // first name
      tokens(2), // middle name
      tokens(3), // last name
      tokens(4), // gender
      Date.valueOf(tokens(5)),  // dob
      tokens(6), // ssn
      tokens(7).toInt // salary
    )
  }

  def peopleAges(): DStream[(String, Int)] = readPeople().map { person =>
    val age = Period.between(person.birthDate.toLocalDate, LocalDate.now()).getYears
    (s"${person.firstName} ${person.lastName}", age)
  }

  def peopleSmallNames(): DStream[String] = readPeople().flatMap { person =>
    List(person.firstName, person.middleName)
  }

  def highIncomePeople() = readPeople().filter(_.salary > 8000)

  // count
  def countPeople(): DStream[Long] = readPeople().count()

  // count by value
  def countNames(): DStream[(String, Long)] = readPeople().map(_.firstName).countByValue()

  // reduce by key
  def countNamesReduce(): DStream[(String, Long)] =
    readPeople()
      .map(_.firstName)
      .map(name => (name, 1L))
      .reduceByKey((a, b) => a + b)

  import spark.implicits._
  def saveToJSON() = readPeople().foreachRDD { rdd =>
    val ds = spark.createDataset(rdd)
    val f = new File("src/main/resources/data/people")
    val nFiles = f.listFiles().length
    val path = s"src/main/resources/data/people/people$nFiles.json"

    ds.write.json(path)
  }


  def main(args: Array[String]): Unit = {
    // val stream = countNamesReduce()
    // stream.print()
    saveToJSON()
    ssc.start()
    ssc.awaitTermination()

  }

}
