package com.mcit.streaming

import com.mcit.utility.Base
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StopTimesConsumer extends App with Base {

  // If using spark-submit script, the master should not be set
  val sparkConf = new SparkConf()
    .setAppName("Practice Spark Streaming")
    .setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
  val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

  val kafkaConfig = Map[String, String] (
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.GROUP_ID_CONFIG -> "group-id-2",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"

  )
  val topic = "stop_times"
  val messages: InputDStream[ConsumerRecord[String, String]] =
    KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String, String](List(topic), kafkaConfig))

  val lines: DStream[String] = messages.map(_.value())

  lines.foreachRDD( rdd => {
    rdd.flatMap(_.split(" ")).take(100).foreach(println)
  })

  ssc.start()
  ssc.awaitTermination()
}
