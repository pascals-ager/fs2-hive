package io.pascals.fs2.hive.connectors

import cats.implicits._
import cats.effect.IO
import fs2.{Chunk, Pull, Stream}
import io.pascals.fs2.hive.domain._
import io.pascals.fs2.hive.tags._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.streaming.{StrictDelimitedInputWriter, StrictJsonWriter}
import java.sql.{Timestamp => SqlTimestamp}

import org.scalatest.{FunSuite, Matchers}

class TestHiveStreamingSink extends FunSuite with Matchers {

  val HIVE_CONF_PATH = "src/test/resources/hive-site.xml"
  val hiveConf       = new HiveConf()
  hiveConf.addResource(new Path(HIVE_CONF_PATH))

  test(
    "HiveStreamingSink Resource acquisition with one WpT Simple Writer",
    Fs2BindTest
  ) {
    val dbName  = "test_db"
    val tblName = "simple_alerts"
    val writer: StrictDelimitedInputWriter = StrictDelimitedInputWriter
      .newBuilder()
      .withFieldDelimiter(',')
      .build()

    val stream: Stream[IO, String] = Stream(
      "17,HiveStreamingSink Resource acquisition,testContinent,testCountry",
      "18,HiveStreamingSink Resource acquisition,testContinent,testCountry",
      "19,HiveStreamingSink Resource acquisition,testContinent,testCountry",
      "20,HiveStreamingSink Resource acquisition,testContinent,testCountry"
    ).covary[IO]

    Stream
      .resource(HiveStreamingSink.create[IO](dbName, tblName, writer, hiveConf))
      .flatMap { hive =>
        stream
          .map(s => s.getBytes)
          .evalMap(in => hive.write(in))
          .adaptErr { case e => fail(s"My exception occurred. ${e}") }
      }
      .compile
      .drain
      .unsafeRunSync()
  }

  test(
    "HiveStreamingSink Resource acquisition with One WpT JsonWriter",
    Fs2BindTest
  ) {
    val dbName  = "test_db"
    val tblName = "alerts"
    val writer: StrictJsonWriter = StrictJsonWriter
      .newBuilder()
      .build()

    import Alerts._
    import io.circe.syntax._

    val alertsRecord: Alerts = Alerts(
      34,
      "34",
      "HiveStreamingSink Resource acquisition with One WpT JsonWriter",
      "Europe",
      "Poland",
      SqlTimestamp.valueOf("2019-11-16 13:02:03.456"),
      2019,
      11,
      16
    )

    val stream: Stream[IO, Alerts] =
      Stream(alertsRecord, alertsRecord, alertsRecord, alertsRecord).covary[IO]

    Stream
      .resource(HiveStreamingSink.create[IO](dbName, tblName, writer, hiveConf))
      .flatMap { hive =>
        stream
          .map(s => s.asJson.toString().getBytes)
          .evalMap(in => hive.write(in))
          .adaptErr { case e => fail(s"My exception occurred. ${e}") }
      }
      .compile
      .drain
      .unsafeRunSync()
  }
}
