package io.pascals.fs2.hive

import java.util.ArrayList
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.streaming.{HiveStreamingConnection, StrictDelimitedInputWriter}

object Fs2StreamHive extends App {

  private val TEST_CONF_PATH = "src/main/resources/hive-site.xml"
  val testConf = new Configuration()
  testConf.addResource(new Path(TEST_CONF_PATH))
  val hiveConf = new HiveConf()
  hiveConf.addResource(testConf)
  val dbName = "test_db"
  val tblName = "alerts"
  val partitionVals = new ArrayList[String](2)
  partitionVals.add("Asia")
  partitionVals.add("India")
  val serdeClass = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"

  val  writer = StrictDelimitedInputWriter.newBuilder()
    .withFieldDelimiter(',')
    .build()

  val connection = HiveStreamingConnection.newBuilder()
    .withDatabase(dbName)
    .withTable(tblName)
    .withStaticPartitionValues(partitionVals)
    .withAgentInfo("test-agent-1")
    .withRecordWriter(writer)
    .withHiveConf(hiveConf)
    .connect()

  // begin a transaction, write records and commit 1st transaction// begin a transaction, write records and commit 1st transaction

  connection.beginTransaction
  connection.write("1,val1".getBytes)
  connection.write("2,val2".getBytes)
  connection.commitTransaction
  // begin another transaction, write more records and commit 2nd transaction
  connection.beginTransaction
  connection.write("3,val3".getBytes)
  connection.write("4,val4".getBytes)
  connection.commitTransaction
  // close the streaming connection
  connection.close()
}
