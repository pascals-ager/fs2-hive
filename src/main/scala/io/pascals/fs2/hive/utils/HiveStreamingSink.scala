package io.pascals.fs2.hive.utils

import cats.effect.{Resource, Sync}
import fs2.Chunk
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.streaming.{AbstractRecordWriter, HiveStreamingConnection}

trait HiveStreamingSink[F[_]] {
  def write(in: Chunk[Byte]) : F[Unit]
}

object HiveStreamingSink {
  def create[F[_]](dbName: String,
                   tblName: String,
                   writer: AbstractRecordWriter,
                   hconf: HiveConf)
                  (implicit F : Sync[F]): Resource[F, HiveStreamingSink[F]] = {
    val open = F.delay {
       HiveStreamingConnection.newBuilder()
        .withDatabase(dbName)
        .withTable(tblName)
        .withAgentInfo("test-agent")
        .withStreamingOptimizations(true)
        .withRecordWriter(writer)
        .withHiveConf(hconf)
        .connect()
    }

    val close = (con: HiveStreamingConnection) => F.delay(con.close())

    Resource.make(open)(close).map { con =>
      new HiveStreamingSink[F] {
        def write(in: Chunk[Byte]) = F.delay {
            con.beginTransaction()
            con.write(in.toBytes.toArray)
            con.commitTransaction()
        }
      }
    }
  }
}