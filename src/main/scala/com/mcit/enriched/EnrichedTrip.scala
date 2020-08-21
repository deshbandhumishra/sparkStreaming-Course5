package com.mcit.enriched

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType}

object EnrichedTrip extends  App {
  val tripfile: String = "resource/trips.txt"
  val frequenciesfile = "resource/frequencies.txt"
  val calendar_datesfile = "resource/calendar_dates.txt"

  //route_id,service_id,trip_id,trip_headsign,direction_id,shape_id,wheelchair_accessible,note_fr,note_en
val trip_schema = new StructType()
    .add("route_id", StringType)
    .add("service_id", StringType)
    .add("trip_id", StringType)
    .add("trip_headsign", StringType)
    .add("direction_id", StringType)
    .add("shape_id", StringType)
    .add("wheelchair_accessible", StringType)
    .add("note_fr", StringType)
    .add("note_en", StringType)

  //trip_id,start_time,end_time,headway_secs
  val frequencies_schema = new StructType()
  .add("trip_id", StringType)
  .add("start_time", StringType)
  .add("end_time", StringType)
  .add("headway_secs", StringType)

 //service_id,date,exception_type
val calendar_dates_schema = new StructType()
    .add("service_id", StringType)
    .add("date", StringType)
    .add("exception_type", StringType)

  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("Course5").getOrCreate()

  val trip_df = spark.read.option("inferSchema", "false").option("header", "true").schema(trip_schema).csv(tripfile)
  val frequencies_df = spark.read.option("inferSchema", "false").option("header", "true").schema(frequencies_schema).csv(frequenciesfile)
  val calendar_dates_df = spark.read.option("inferSchema", "false").option("header", "true").schema(calendar_dates_schema).csv(calendar_datesfile)

  printf("Ishrath")
  trip_df.show(10)
  frequencies_df.show(10)
  calendar_dates_df.show(10)

  println("Azhar")
  trip_df.printSchema
  frequencies_df.printSchema
  calendar_dates_df.printSchema

  println("Shah")
  trip_df.createTempView("trip_view")
  frequencies_df.createTempView("frequencies_view")
  calendar_dates_df.createTempView("calendar_dates_view")


  val enriched_trip: Unit = spark.sql(""" select route_id,trip_view.service_id,trip_view.trip_id,trip_headsign,direction_id,shape_id,wheelchair_accessible,note_fr,note_en,start_time,end_time,headway_secs,date,exception_type from trip_view,frequencies_view,calendar_dates_view where trip_view.trip_id = frequencies_view.trip_id and trip_view.service_id=calendar_dates_view.service_id""" ).show(10)





}
