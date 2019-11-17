package io.pascals.fs2.hive.utils

import cats.effect
import cats.implicits._
import cats.effect.{ExitCode, IO, IOApp, Resource}
import fs2.{Chunk, Stream}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.streaming.{HiveStreamingConnection, StrictDelimitedInputWriter, StrictJsonWriter}

object TestHiveStreamingSink extends IOApp {

  override def run(args: List[String]): IO[effect.ExitCode] = {
    val HIVE_CONF_PATH = "src/test/resources/hive-site.xml"
    val hiveConf = new HiveConf()
    hiveConf.addResource(new Path(HIVE_CONF_PATH))

    val dbName = "test_db"
    val tblName = "simple_alerts"
    val writer: StrictDelimitedInputWriter = StrictDelimitedInputWriter.newBuilder()
      .withFieldDelimiter(',')
      .build()
    //  And eventually the call itself:
    val stream: Stream[IO, String] = Stream(
      "17,Fs2StreamBinding,testContinent,testCountry",
      "18,Fs2StreamBinding,testContinent,testCountry",
      "19,Fs2StreamBinding,testContinent,testCountry",
      "20,Fs2StreamBinding,testContinent,testCountry"
    ).covary[IO]

    Stream.resource(HiveStreamingSink.create[IO](dbName,tblName,writer,hiveConf)).flatMap { hive =>
      stream
        .map(s => Chunk.bytes(s.getBytes))
        .evalMap(in => hive.write(in))
        .adaptErr { case e => new Exception(s"My exception occurred. ${e}") }
    }.compile.drain.as(ExitCode.Success)
  }
}