package io.pascals.fs2.hive.utils

import cats.effect.{ConcurrentEffect, Sync}
import fs2.{Chunk, Pipe, Pull, Stream}

trait StreamingSink[F[_], I] {
  def writeSink(in: I) : Unit
}

object StreamingSink {
  def apply[F[_], I](implicit writer: StreamingSink[F, I], F: Sync[F]): Pipe[F, I, Unit] =
    in =>
      in.repeatPull {
        _.uncons.flatMap {
          case Some((hd: Chunk[I], tl: Stream[F, I])) => Pull.output(hd.map { item =>
            writer.writeSink(item)
          }).as(Some(tl))
          case None => Pull.pure(None)
        }
      }

}