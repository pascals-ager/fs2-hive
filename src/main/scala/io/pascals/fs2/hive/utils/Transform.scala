package io.pascals.fs2.hive.utils

import cats.effect.ConcurrentEffect
import fs2.{Chunk, Pipe, Pull, Stream}

trait Transform[F[_], I, O] {
  def transformation(in: I): O
}

object Transform {
  def apply[F[_], A, B](
      implicit transformer: Transform[F, A, B],
      F: ConcurrentEffect[F]
  ): Pipe[F, A, B] =
    in =>
      in.repeatPull {
        _.uncons.flatMap {
          case Some((hd: Chunk[A], tl: Stream[F, A])) =>
            Pull
              .output(hd.map { idc =>
                transformer.transformation(idc)
              })
              .as(Some(tl))
          case None => Pull.pure(None)
        }
      }
}
