name := "sparkKafkaStreamingCourse5"

version := "0.1"

scalaVersion := "2.12.10"


//resolvers += "Cloudera" at "http://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.1.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0"
