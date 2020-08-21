package com.mcit.streaming

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

import scala.Console.println

object StopTimesProducer extends App{

  val stop_timesfile: String = "resource/stop_times.txt"

  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("StreamingCourse5").getOrCreate()
  val stop_times = spark.sparkContext.textFile(stop_timesfile)

  val logger:Logger = LogManager.getLogger("StopTimesProducer")
   logger.info("Creating Kafka Producer...")

  val topicName = "stop_times"

  val producerProperties = new Properties()
  producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
  producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val producer = new KafkaProducer[Int, String](producerProperties)

  stop_times.foreach{row =>
    println("====================="+row)
    producer.send(new ProducerRecord[Int, String](topicName, 100, row))
    }
  producer.flush()

  def customPartitioned(key: Int): Int = key % 3

  logger.info("Finished - Closing Kafka Producer.")
  producer.close()

}