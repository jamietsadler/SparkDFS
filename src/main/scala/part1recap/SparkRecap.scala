package part1recap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, expr, sum, stddev, min, max, mean}

object SparkRecap {

  val spark = SparkSession.builder()
    .appName("Spark Recap")
    .config("spark.master","local")
    .getOrCreate()

  // read a DF
  val cars = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars") // pass in folder and spark will read in all files in folder.

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    cars.show()
    cars.printSchema()

    cars.select(
      col("Name"),// Column object
      $"Year", // another column object, needs implicits
      (col("Weight_in_lbs")/2.2).as("Weight_in_kgs"),
      expr("Weight_in_lbs / 2.2").as("Weight_in_kgs2")
    )

    val carWeights = cars.selectExpr("Weight_in_lbs / 2.2")

    val europeanCars = cars.where(col("Origin") =!= "USA")

    val averageHP = cars.select(avg(col("Horsepower")).as("average_hp")) // sum, mean, stddev, min, max
    println(averageHP)


    cars.groupBy(col("Origin"))
      .count()
      .show()

  }


}
