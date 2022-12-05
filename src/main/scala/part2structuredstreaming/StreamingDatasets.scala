package part2structuredstreaming

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import common._

object StreamingDatasets {

  val spark = SparkSession.builder()
    .appName("Streaming Datasets")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  // Streaming DSs support functional operators

  def readCars(): Dataset[Car] = {
    val carEncoder = Encoders.product[Car] // useful for DF to DS transformations

    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), carsSchema).as("car"))
      .selectExpr("car.*") // DF with multiple columns
      // .as[Car](carEncoder) // state explicitly
      .as[Car] // uses implicits
  }

  def showCarNames() = {
    val carsDS: Dataset[Car] = readCars()

    // transformations
    val carNamesDF: DataFrame = carsDS.select(col("Name"))

    // maintains type info
    val carNamesAlt: Dataset[String] = carsDS.map(_.Name)

    carNamesAlt.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def ex1() = {
    val carsDS = readCars()
    carsDS.filter(_.Horsepower.getOrElse(0L) > 140)
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def ex2() = {
    val carsDS = readCars()
    carsDS.select(avg(col("HorsePower")))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }

  def ex3() = {
    val carsDS = readCars()

    val carsCountByOrigin = carsDS.groupBy(col("Origin")).count()
    val carsCountByOriginAlt =  carsDS.groupByKey(car => car.Origin).count()

    carsCountByOriginAlt
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    ex2()
  }

}
