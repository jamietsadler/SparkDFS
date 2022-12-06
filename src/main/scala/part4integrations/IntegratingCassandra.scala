package part4integrations

import org.apache.spark.sql.SparkSession

object IntegratingCassandra {

  val spark = SparkSession.builder()
    .appName("Integrating Cssandra")
    .master("local[2]")
    .getOrCreate()

  def writeStreamToCassandraInBatches() = {
    val carsDS = spark.readStream

  }

  def main(args: Array[String]): Unit = {

  }

}
