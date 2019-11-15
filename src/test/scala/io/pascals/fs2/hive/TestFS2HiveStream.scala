package io.pascals.fs2.hive

import cats.effect.IO
import fs2._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.streaming.{HiveStreamingConnection, StrictDelimitedInputWriter}
import io.pascals.fs2.hive.tags.{IntegrationTest, InvalidTableTest, InvocationTargetExceptionTest}
import org.scalatest.{FunSuite, Matchers}

class TestFS2HiveStream extends FunSuite with Matchers {

  private val HIVE_CONF_PATH = "src/test/resources/hive-site.xml"
  val hiveConf = new HiveConf()
  hiveConf.addResource(new Path(HIVE_CONF_PATH))

  test("Simple Streaming Connection Test", IntegrationTest) {
    val dbName = "test_db"
    val tblName = "alerts"
    val writer: StrictDelimitedInputWriter = StrictDelimitedInputWriter.newBuilder()
      .withFieldDelimiter(',')
      .build()

    val builder: HiveStreamingConnection.Builder = HiveStreamingConnection.newBuilder()
      .withDatabase(dbName)
      .withTable(tblName)
      .withAgentInfo("hive")
      .withStreamingOptimizations(true)
      .withRecordWriter(writer)
      .withHiveConf(hiveConf)

    val f: Stream[IO, Unit] = Stream.bracket(IO.delay(builder.connect()))(conn => IO.delay(conn.close())).map{
      conn =>
      { conn.beginTransaction()
        conn.write("17,SimpleStreaming,12,Africa,Nigeria".getBytes())
        conn.commitTransaction()
      }
    }

    f.handleErrorWith{
      f => Stream.emit{
        fail(s"Exception occurred. ${f.getCause}")
      }
    }.compile.drain.unsafeRunSync()
  }

  test("InvalidTable Exception Test", InvalidTableTest) {
    val dbName = "test"
    val tblName = "alerts"
    val writer: StrictDelimitedInputWriter = StrictDelimitedInputWriter.newBuilder()
      .withFieldDelimiter(',')
      .build()

    val builder: HiveStreamingConnection.Builder = HiveStreamingConnection.newBuilder()
      .withDatabase(dbName)
      .withTable(tblName)
      .withAgentInfo("hive")
      .withStreamingOptimizations(true)
      .withRecordWriter(writer)
      .withHiveConf(hiveConf)

    val f: Stream[IO, Unit] = Stream.bracket(IO.delay(builder.connect()))(conn => IO.delay(conn.close())).map{
      conn =>
      { conn.beginTransaction()
        conn.write("17,InvalidTable,Africa,Nigeria".getBytes())
        conn.commitTransaction()
      }
    }

    f.handleErrorWith{
      f => Stream.emit{
        assert(f.isInstanceOf[org.apache.hive.streaming.InvalidTable])
      }
    }.compile.drain.unsafeRunSync()
  }

  test("InvocationTargetException Test", InvocationTargetExceptionTest) {

    hiveConf.set("hive.metastore.uris", "thrift://hive-metastore-three:9084")
    val dbName = "test_db"
    val tblName = "alerts"
    val writer: StrictDelimitedInputWriter = StrictDelimitedInputWriter.newBuilder()
      .withFieldDelimiter(',')
      .build()

    val builder: HiveStreamingConnection.Builder = HiveStreamingConnection.newBuilder()
      .withDatabase(dbName)
      .withTable(tblName)
      .withAgentInfo("hive")
      .withStreamingOptimizations(true)
      .withRecordWriter(writer)
      .withHiveConf(hiveConf)

    val f: Stream[IO, Unit] = Stream.bracket(IO.delay(builder.connect()))(conn => IO.delay(conn.close())).map{
      conn =>
      { conn.beginTransaction()
        conn.write("17,InvocationTargetException,Africa,Nigeria".getBytes())
        conn.commitTransaction()
      }
    }

    f.handleErrorWith{
      f => Stream.emit{
        assert(f.isInstanceOf[java.lang.RuntimeException])
        assert(f.getCause.isInstanceOf[java.lang.reflect.InvocationTargetException])
        assert(f.getLocalizedMessage == "Unable to instantiate org.apache.hadoop.hive.metastore.HiveMetaStoreClient")
      }
    }.compile.drain.unsafeRunSync()
  }

}