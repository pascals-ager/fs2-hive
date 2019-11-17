package io.pascals.fs2.hive.utils

import java.sql.{Timestamp => SqlTimestamp}
import cats.effect.IO
import fs2.Stream
import io.circe.Decoder.Result
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder, HCursor, Json}
import io.circe.syntax._
import io.pascals.fs2.hive.tags.Fs2BindTest
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.streaming.{HiveStreamingConnection, StrictDelimitedInputWriter, StrictJsonWriter}
import org.scalatest.{FunSuite, Matchers}

class TestStreamingSink extends FunSuite with Matchers {

  val HIVE_CONF_PATH = "src/test/resources/hive-site.xml"
  val hiveConf = new HiveConf()
  hiveConf.addResource(new Path(HIVE_CONF_PATH))

  test("Fs2Binding with one delimited WpT", Fs2BindTest)  {
    val dbName = "test_db"
    val tblName = "simple_alerts"
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

    val stream: Stream[IO, String] = Stream("17,Fs2Binding with one delimited WpT,Africa,Nigeria",
      "18,Fs2Binding with one delimited WpT,Africa,Congo",
      "19,Fs2Binding with one delimited WpT,Africa,Egypt",
      "20,Fs2Binding with one delimited WpT,Africa,Zimbabwe").covary[IO]

    stream
      .through(StreamingSink[IO, String])
      .handleErrorWith{
        f => Stream.emit{
        fail(s"Exception occurred. ${f}")
      }
    }.compile.drain.unsafeRunSync()
  }

  test("Fs2Binding with one Json WpT Test", Fs2BindTest)  {
    val dbName = "test_db"
    val tblName = "alerts"
    val jsonWriter: StrictJsonWriter = StrictJsonWriter.newBuilder()
      .build()

    case class Alerts(id: Int,
                      tracking_id: String,
                      msg: String,
                      continent: String,
                      country: String,
                      event_time: SqlTimestamp,
                      year: Int,
                      month: Int,
                      day: Int)



    implicit val SqlTimestampFormat : Encoder[SqlTimestamp] with Decoder[SqlTimestamp] = new Encoder[SqlTimestamp] with Decoder[SqlTimestamp] {
      override def apply(a: SqlTimestamp): Json = Encoder.encodeString.apply(a.toString)

      override def apply(c: HCursor): Result[SqlTimestamp] = Decoder.decodeString.map(s => SqlTimestamp.valueOf(s) ).apply(c)
    }

    implicit val alertsDecoder: Decoder[Alerts] = deriveDecoder[Alerts]
    implicit val alertsEncoder: Encoder[Alerts] = deriveEncoder[Alerts]

    val alertsRecord: Alerts = Alerts(34, "34", "Fs2Binding with one Json WpT Test", "Europe", "Germany", SqlTimestamp.valueOf("2019-11-16 13:02:03.456"), 2019, 11, 16)

    implicit def hiveSink: StreamingSink[IO, Alerts] = ( in: Alerts ) => IO.delay {
      val con = HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tblName)
        .withAgentInfo("hive")
        .withStreamingOptimizations(true)
        .withRecordWriter(jsonWriter)
        .withHiveConf(hiveConf)
        .connect()
      con.beginTransaction()
      con.write(in.asJson.toString().getBytes())
      con.commitTransaction()
      con.close()
    }

    val stream: Stream[IO, Alerts] = Stream(alertsRecord,
      alertsRecord,
      alertsRecord,
      alertsRecord).covary[IO]

    stream
      .through(StreamingSink[IO, Alerts])
      .handleErrorWith{
        f => Stream.emit{
          fail(s"Exception occurred. ${f}")
        }
      }.compile.drain.unsafeRunSync()
  }


}
