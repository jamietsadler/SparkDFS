package part5advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object EventTimeWindows {

  val spark = SparkSession.builder()
    .appName("Event Time Windows")
    .master("local[2]")
    .getOrCreate()

  val onlinePurchaseSchema = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType)
  ))

  def readPurchasesFromSocket() = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()
    .select(from_json(col("value"), onlinePurchaseSchema).as("purchase"))
    .selectExpr("purchase.*")

  def aggregatePurchasesBySlidingWindow() = {
    val purchasesDF = readPurchasesFromSocket()
    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day", "1 hour").as("time"))
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  // Show best selling product of every day
  def bestSellingPerDay() = {
    val purchasesDF = readPurchasesFromSocket()

    val bestSelling = purchasesDF
      .groupBy(col("item"), window(col("time"), "1 day", "1 hour").as("day"))
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("item"),
        col("totalQuantity")
      )
      .orderBy(col("day"), col("totalQuantity").desc)

    bestSelling.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }



  def main(args: Array[String]): Unit = {
    bestSellingPerDay()
  }

}
