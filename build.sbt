name := "monixfoo"

version := "1.0"

scalaVersion := "2.12.1"


// for the JVM
libraryDependencies ++= Seq("io.monix" %% "monix" % "2.1.2",
"org.typelevel" %% "cats" % "0.8.1",
"io.monix" %% "monix-cats" % "2.1.2",
"com.typesafe.akka" %% "akka-actor" % "2.4.16",
"com.typesafe.akka" %% "akka-typed-experimental" % "2.4.16",
"com.typesafe.akka" %% "akka-stream" % "2.4.16",
"io.swave" %% "swave-core" % "0.6.0",
"co.fs2" %% "fs2-core" % "0.9.2")
