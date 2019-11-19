package io.pascals.fs2.hive.utils

import cats.effect.{ExitCase, Resource, Sync}
import fs2.Chunk
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.streaming.{AbstractRecordWriter, HiveStreamingConnection}

trait StreamChunkSink[F[_]] {
  def write( in: Chunk[Byte] ): F[Unit]
}

object StreamChunkSink {
  def create[F[_]]( dbName: String,
                    tblName: String,
                    writer: AbstractRecordWriter,
                    hconf: HiveConf )
                  ( implicit F: Sync[F] ): Resource[F, StreamChunkSink[F]] = {
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

    Resource.makeCase(open){
      case (con, ExitCase.Completed) => F.delay {
        con.commitTransaction()
        con.close()
      }
      case (con, ExitCase.Error(_) | ExitCase.Canceled ) => F.delay{
        con.abortTransaction()
        con.close()
      }
    }.map { con =>
      con.beginTransaction()
      new StreamChunkSink[F] {
        override def write( in: Chunk[Byte] ): F[Unit] = F.delay{
          con.write(in.toBytes.toArray)
        }
      }
    }
  }
}