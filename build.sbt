oneJarSettings

name := "kafkaStart"

version := "1.0"

scalaVersion := "2.10.4"


libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.2.0"

libraryDependencies += "net.liftweb" % "lift-json_2.10" % "3.0-M1" withSources() withJavadoc()

libraryDependencies += "com.typesafe.slick" % "slick-codegen_2.10" % "2.1.0" withSources() withJavadoc()

libraryDependencies += "com.typesafe.slick" % "slick_2.10" % "2.1.0" withSources() withJavadoc()
