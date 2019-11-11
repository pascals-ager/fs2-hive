package io.pascals.fs2.hive

import java.util

import scala.collection.JavaConverters._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.streaming.{HiveStreamingConnection, StrictDelimitedInputWriter}

import scala.collection.mutable.ArrayBuffer

object Fs2StreamHive extends App {

  private val TEST_CONF_PATH = "src/main/resources/hive-site.xml"
  val hiveConf = new HiveConf()
  hiveConf.addResource(new Path(TEST_CONF_PATH))
  val dbName = "test_db"
  val tblName = "alerts"
  val partitionVals = new util.ArrayList[String](2)
  partitionVals.add("Asia")
  partitionVals.add("China")
  //val serdeClass = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"

  val  writer = StrictDelimitedInputWriter.newBuilder()
    .withFieldDelimiter(',')
    .build()

  val connection: HiveStreamingConnection = HiveStreamingConnection.newBuilder()
    .withDatabase(dbName)
    .withTable(tblName)
    .withStaticPartitionValues(partitionVals)
    .withAgentInfo("hive")
    .withRecordWriter(writer)
    .withHiveConf(hiveConf)
    .connect()

  // begin a transaction, write records and commit 1st transaction
try {
  connection.beginTransaction()

  connection.write("11,value11".getBytes)
  connection.write("12,value12".getBytes)
  connection.commitTransaction()
  // begin another transaction, write more records and commit 2nd transaction
  connection.beginTransaction()
  connection.write("13,value13".getBytes())
  connection.write("14,value14".getBytes())
  connection.commitTransaction()
  // close the streaming connection
}
  catch {
    case (e : Exception)  => println(s"Exception occurred $e")
      //Exception occurred org.apache.hive.streaming.StreamingIOFailure: Unable to create partition: [Asia, China]for { metaStoreUri: thrift://hive-metastore-three:9083, database: test_db, table: alerts }

  }
  finally {
  connection.close()
  }
}
