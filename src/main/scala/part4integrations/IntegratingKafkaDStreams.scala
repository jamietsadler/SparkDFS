package part4integrations

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object IntegratingKafkaDStreams {

  val spark = SparkSession.builder()
    .appName("Integrating Kafka and DStreams")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer],
    "value.serializer" -> classOf[StringSerializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit"-> false.asInstanceOf[Object]
  )

  val kafkaTopic = "rockthejvm"

  def readFromKafka() = {
    val topics = Array(kafkaTopic)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, // distribute partitions evenly across cluster
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
    )

    val processedStream = kafkaDStream.map(record => (record.key(), record.value()))
    processedStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def writeToKafka() = {
    val inputData = ssc.socketTextStream("localhost", 12345)

    // transform data
    val processedData = inputData.map(_.toUpperCase())

    processedData.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        // inside this lambda, code run by single executor
        val kafkaHashMap = new java.util.HashMap[String, Object]()
        kafkaParams.foreach { pair =>
          kafkaHashMap.put(pair._1, pair._2)
        }

        // producer available on executor and can insrt records into Kafka topics
        val producer = new KafkaProducer[String, String](kafkaHashMap)

        partition.foreach { value =>
          val message = new ProducerRecord[String, String](kafkaTopic, null, value)
          producer.send(message)
        }
        producer.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    writeToKafka()
  }

}
