package io.pascals.fs2.hive.utils

import java.sql.{Timestamp => SqlTimestamp}

import cats.effect.IO
import fs2.{Chunk, Stream}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.streaming.StrictJsonWriter
import io.pascals.fs2.hive.domain._
import io.pascals.fs2.hive.tags._
import org.scalatest.{FunSuite, Matchers}

class TestStreamChunkSink extends FunSuite with Matchers {

  val HIVE_CONF_PATH = "src/test/resources/hive-site.xml"
  val hiveConf = new HiveConf()
  hiveConf.addResource(new Path(HIVE_CONF_PATH))

  test("StreamChunkSink Chunk WpT Click JsonWriter", ChunkWriteTest)  {
    val dbName = "test_db"
    val tblName = "clicks_with_struct_array"
    val writer: StrictJsonWriter = StrictJsonWriter.newBuilder()
      .build()

    import ClickWithStructArray._
    import io.circe.syntax._
    val clickWithStructArrayRecord: ClickWithStructArray = ClickWithStructArray("Clicks",
      "14ed5f",
      Some("72e4df"),
      SqlTimestamp.valueOf("2019-11-18 11:02:03.456"),
      SqlTimestamp.valueOf("2019-11-18 11:02:03.456"),
      "72e4df",
      SourceAttributesWithArray(Some("14ed5f"), Some("Clicks"),
        AttributesDataWithArray(Some("nonsense"),
          Some("http://nonsense.com"),
          Some(Map("header" -> "http")),
          Some(Map("request" -> List("tracked, linked, authenticated")))
        ),
        Some(Map("external_data" -> "3ps"))),
      Some(List(Strategy(Some("Real-Time Tracker"), Some(100), Some("ok"), Some("1 request pm")))),
      2019,
      11,
      16
    )

    val stream: Stream[IO, ClickWithStructArray] = Stream(clickWithStructArrayRecord)
      .repeatN(100000)
      .covary[IO]


    Stream.resource(StreamChunkSink.create[IO](dbName,tblName,writer,hiveConf)).flatMap { hive =>
      stream
        .map(s => Chunk.bytes(s.asJson.toString().getBytes))
        .evalMap(in => hive.write(in))
    }.compile.drain.unsafeRunSync()
  }
}
