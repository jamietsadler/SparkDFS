package part2structuredstreaming

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object StreamingAggregations {

  val spark = SparkSession.builder()
    .appName("Streaming Aggregations")
    .master("local[2]")
    .getOrCreate()

  def streamingCount() = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val lineCount: DataFrame = lines.selectExpr("count(*) as lineCount")

    // aggregations with distinct not supported

    lineCount.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def numericalAggregations(): Unit = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val numbers = lines.select(col("value").cast("integer").as("number"))
    val sumDF = numbers.select(sum(col("number")).as("agg_so_far"))

    sumDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }

  def groupNames(): Unit = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val groups = lines.select(
      col("value").as("name")
    ).groupBy(col("name")) // relational grouped dataset
      .count()

    groups.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    groupNames()
  }

}
