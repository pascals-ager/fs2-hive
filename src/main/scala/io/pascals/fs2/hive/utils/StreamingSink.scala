package io.pascals.fs2.hive.utils

import cats.effect
import cats.effect.{ConcurrentEffect, IO, Resource, Sync}
import fs2.{Chunk, Pipe, Pull, Stream}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.streaming.{HiveStreamingConnection, StrictJsonWriter}

trait StreamingSink[F[_], I] {
  def writeSink(in: I) : Unit
}

object StreamingSink {

  def apply[F[_], I](implicit writer: StreamingSink[F, I], F: Sync[F]): Pipe[F, I, Unit] =
    in =>
      in.repeatPull {
        _.uncons.flatMap {
          case Some((hd: Chunk[I], tl: Stream[F, I])) =>
            Pull.output( // Maybe do resource acquisition here? (including begin transaction)
              hd.map { item =>
                writer.writeSink(item)
            })
              .as(Some(tl))
          case None => Pull.pure(None)
        }
    }
}
