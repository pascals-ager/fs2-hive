package io.pascals.fs2.hive.utils

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import fs2.Stream
import io.pascals.fs2.hive.tags.Fs2BindTest
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.streaming.{HiveStreamingConnection, StrictDelimitedInputWriter}
import org.scalatest.{FunSuite, Matchers}

class TestStreamingSink extends FunSuite with Matchers {

  val HIVE_CONF_PATH = "src/test/resources/hive-site.xml"
  val hiveConf = new HiveConf()
  hiveConf.addResource(new Path(HIVE_CONF_PATH))

  test("Fs2Binding with one write per transaction Test", Fs2BindTest)  {
    val dbName = "test_db"
    val tblName = "alerts"
    val writer: StrictDelimitedInputWriter = StrictDelimitedInputWriter.newBuilder()
      .withFieldDelimiter(',')
      .build()

    implicit def hiveSink: StreamingSink[IO, String] = ( in: String ) => {
      val con = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tblName)
        .withAgentInfo("hive")
        .withStreamingOptimizations(true)
        .withRecordWriter(writer)
        .withHiveConf(hiveConf)
        .connect()
      con.beginTransaction()
      con.write(in.getBytes)
      con.commitTransaction()
      con.close()
    }

    val stream: Stream[IO, String] = Stream("17,Fs2StreamBinding,Africa,Nigeria",
      "18,Fs2StreamBinding,Africa,Congo",
      "19,Fs2StreamBinding,Africa,Egypt",
      "20,Fs2StreamBinding,Africa,Zimbabwe").covary[IO]

    stream
      .through(StreamingSink[IO, String])
      .handleErrorWith{
        f => Stream.emit{
        fail(s"Exception occurred. ${f}")
      }
    }.compile.drain.unsafeRunSync()
  }
}
