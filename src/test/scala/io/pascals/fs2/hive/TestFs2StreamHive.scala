package io.pascals.fs2.hive

import java.util

import cats.effect.IO
import fs2._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException
import org.apache.hive.streaming.{HiveStreamingConnection, StrictDelimitedInputWriter}
import org.scalatest.{Assertion, FunSuite, Matchers}

import scala.util.Try

class TestFs2StreamHive extends FunSuite with Matchers {


  private val HIVE_CONF_PATH = "src/test/resources/hive-site.xml"

  val hiveConf = new HiveConf()
  hiveConf.addResource(new Path(HIVE_CONF_PATH))

  val dbName = "test_db"
  val tblName = "alerts"

  test("Simple Streaming Connection Test") {

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


    /*
    val test: IO[Unit] = for {
      connection <- IO(builder.connect())
      _ <- IO(connection.beginTransaction())
      _ <- IO(connection.write("13,value13,Asia,India".getBytes()))
    } yield connection.commitTransaction()


    test.unsafeRunSync()

    */


    val f: Stream[IO, Unit] = Stream.bracket(IO.delay(builder.connect()))(conn => IO.delay(conn.close())).map{
      conn =>
      { conn.beginTransaction()
        conn.write("17,value17,Africa,Nigeria".getBytes())
        conn.commitTransaction()
      }
    }

    f.handleErrorWith{
     f => Stream.emit{
       fail(s"Exception occurred. ${f.getCause}")
     }
    }.compile.drain.unsafeRunSync()

    /*
    val connection: HiveStreamingConnection = HiveStreamingConnection.newBuilder()
      .withDatabase(dbName)
      .withTable(tblName)
      .withAgentInfo("hive")
      .withStreamingOptimizations(false)
      .withRecordWriter(writer)
      .withHiveConf(hiveConf)
      .connect()

    // begin a transaction, write records and commit 1st transaction
    try {
      val a = connection.beginTransaction()
      connection.write("11,value11,Asia,India".getBytes())
      // connection.write("12,value12".getBytes())

      connection.commitTransaction()
      // begin another transaction, write more records and commit 2nd transaction
      // connection.beginTransaction()
      // connection.write("13,value13".getBytes())
      // connection.write("14,value14".getBytes())
      // connection.commitTransaction()
      // close the streaming connection
    }
    catch {
      case (e: Exception) => {
        assertThrows(s"Exception occurred $e")
      }
    }
    finally {
      connection.close()
    }
    */
  }
}
