package io.pascals.fs2.hive

import java.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.streaming.{HiveStreamingConnection, StrictDelimitedInputWriter}
import org.scalatest.{FunSuite, Matchers}

class TestFs2StreamHive extends FunSuite with Matchers {


  private val HIVE_CONF_PATH = "src/main/resources/hive-site.xml"

  val hiveConf = new HiveConf()
  hiveConf.addResource(new Path(HIVE_CONF_PATH))

  val dbName = "test_db"
  val tblName = "alerts"
  val partitionVals = new util.ArrayList[String](2)
  partitionVals.add("Asia")
  partitionVals.add("China")

  val writer: StrictDelimitedInputWriter = StrictDelimitedInputWriter.newBuilder()
    .withFieldDelimiter(',')
    .build()

  test("Simple Streaming Connection Test") {
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
      connection.beginTransaction()
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
  }
}
