package part4integrations

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{Dataset, ForeachWriter, SaveMode, SparkSession}
import common._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.col

object IntegratingCassandra {

  val spark = SparkSession.builder()
    .appName("Integrating Cssandra")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  def writeStreamToCassandraInBatches() = {
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS
      .writeStream
      .foreachBatch { (batch: Dataset[Car], batchId: Long) =>
        // save batch to cassandra in single table write
        batch.select(col("Name"), col("Horsepower"))
          .write
          .cassandraFormat("cars", "public")
          .mode(SaveMode.Append)
          .save()
      }
      .start()
      .awaitTermination()
  }

  class carCassandraForeachWriter extends ForeachWriter[Car] {

    val keyspace = "public"
    val table = "cars"
    val connector = CassandraConnector(spark.sparkContext.getConf)

    override def open(partitionId: Long, epochId: Long): Boolean = {
      println("Open Connection")
      true
    }
    override def process(car: Car): Unit = {
      connector.withSessionDo { session =>
      session.execute(
        s"""
           |insert into $keyspace.$table("Name", "Horsepower")
           |values ('${car.Name}', ${car.Horsepower}')
           |""".stripMargin
      )
      }
    }
    override def close(errorOrNull: Throwable): Unit = println("closing connection")

  }

  def writeStreamToCassandra() = {
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS
      .writeStream
      .foreach(new carCassandraForeachWriter)
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeStreamToCassandra()
  }

}
