package io.pascals.fs2.hive.connectors

import cats.effect.{ExitCase, Resource, Sync}
import fs2.Chunk
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.streaming.{
  AbstractRecordWriter,
  ConnectionStats,
  HiveStreamingConnection
}

trait HiveStreamingSink[F[_]] {
  def abort(): F[Unit]

  def begin(): F[Unit]

  def commit(): F[Unit]

  def connectionStats(): F[ConnectionStats]

  def write(in: Array[Byte]): F[Unit]

  def writeChunk(in: Chunk[Array[Byte]]): F[Unit]

  def writeChunks(in: Chunk[Chunk[Array[Byte]]]): F[Unit]
}

object HiveStreamingSink {
  def create[F[_]](
      dbName: String,
      tblName: String,
      writer: AbstractRecordWriter,
      hconf: HiveConf
  )(implicit F: Sync[F]): Resource[F, HiveStreamingSink[F]] = {

    val open: F[HiveStreamingConnection] = F.delay {
      val builder = HiveStreamingConnection
        .newBuilder()
        .withDatabase(dbName)
        .withTable(tblName)
        .withAgentInfo("test-agent")
        .withStreamingOptimizations(true)
        .withRecordWriter(writer)
        .withHiveConf(hconf)
      builder.connect()
    }

    Resource
      .makeCase(open) {
        case (con, ExitCase.Completed | ExitCase.Canceled) =>
          F.delay {
            con.close()
          }
        case (con, ExitCase.Error(_)) =>
          F.delay {
            con.close()
          }
      }
      .map { hStreamingConnection =>
        new HiveStreamingSink[F] {

          override def abort(): F[Unit] = F.delay {
            hStreamingConnection.abortTransaction()
          }

          override def begin(): F[Unit] = F.delay {
            hStreamingConnection.beginTransaction()
          }

          override def commit(): F[Unit] = F.delay {
            hStreamingConnection.commitTransaction()
          }

          override def connectionStats(): F[ConnectionStats] = F.delay {
            hStreamingConnection.getConnectionStats
          }

          override def write(in: Array[Byte]): F[Unit] = F.delay {
            hStreamingConnection.beginTransaction()
            hStreamingConnection.write(in)
            hStreamingConnection.commitTransaction()
          }

          override def writeChunk(in: Chunk[Array[Byte]]): F[Unit] = F.delay {
            hStreamingConnection.beginTransaction()
            in.foreach(rec => hStreamingConnection.write(rec))
            hStreamingConnection.commitTransaction()
          }

          override def writeChunks(in: Chunk[Chunk[Array[Byte]]]): F[Unit] =
            F.delay {
              in.foreach { chunk =>
                hStreamingConnection.beginTransaction()
                chunk.foreach(
                  rec => hStreamingConnection.write(rec)
                )
                hStreamingConnection.commitTransaction()
              }
            }
        }
      }
  }
}
