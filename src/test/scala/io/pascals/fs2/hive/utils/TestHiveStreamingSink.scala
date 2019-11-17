package io.pascals.fs2.hive.utils

import cats.effect.{ConcurrentEffect, IO, IOApp, Resource}
import fs2.{Chunk, Stream}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.streaming.{HiveStreamingConnection, StrictJsonWriter}

object TestHiveStreamingSink extends IOApp {

      implicit def create[IO: ConcurrentEffect]: Resource[IO, HiveStreamingSink[IO]] = {
        val open = IO.delay {
          val HIVE_CONF_PATH = "src/test/resources/hive-site.xml"
          val hConf = new HiveConf()
          hConf.addResource(new Path(HIVE_CONF_PATH))
          val dbName = "test_db"
          val tblName = "simple_alerts"
          val jsonWriter: StrictJsonWriter = StrictJsonWriter.newBuilder()
            .build()
          HiveStreamingConnection.newBuilder()
            .withDatabase(dbName)
            .withTable(tblName)
            .withAgentInfo("test-agent")
            .withStreamingOptimizations(true)
            .withRecordWriter(jsonWriter)
            .withHiveConf(hConf)
            .connect()
        }

        implicit val close: HiveStreamingConnection => IO[Unit] = (con: HiveStreamingConnection) => IO.delay {
          con.close()
        }

        Resource.make(open)(close).map { con =>
          (in: Chunk[Byte]) =>
            cats.effect.IO.delay {
              con.beginTransaction()
              con.write(in.toByteBuffer.array())
              con.commitTransaction()
            }
        }
      }

  //  And eventually the call itself:
  val stream: Stream[IO, String] = Stream(
    "17,Fs2StreamBinding,testContinent,testCountry",
    "18,Fs2StreamBinding,testContinent,testCountry",
    "19,Fs2StreamBinding,testContinent,testCountry",
    "20,Fs2StreamBinding,testContinent,testCountry"
  ).covary[IO]

  Stream.resource(create[IO]).flatMap { hive =>
    stream
      .map(s => Chunk.bytes(s.getBytes))
      .evalMap(in => hive.writeSink(in))
      .handleErrorWith {  e => throw new Error(s"My exception occurred. ${e}") }
  }

}
