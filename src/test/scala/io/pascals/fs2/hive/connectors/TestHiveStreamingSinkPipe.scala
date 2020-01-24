package io.pascals.fs2.hive.connectors

import cats.effect.{ContextShift, IO, Timer}
import fs2.{Chunk, Pipe, Pull, Stream}
import fs2.kafka._
import io.circe
import io.circe.parser._
import io.circe.syntax._
import io.pascals.fs2.hive.domain.flatten._
import io.pascals.fs2.hive.domain.kafka.KafkaMetadata
import io.pascals.fs2.hive.tags.IntegrationTest
import io.pascals.fs2.hive.utils.Transform
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.streaming.StrictJsonWriter
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.ExecutionContext

class TestHiveStreamingSinkPipe extends FunSuite with Matchers {

  val HIVE_CONF_PATH = "src/test/resources/hive-site.xml"
  val hiveConf       = new HiveConf()
  hiveConf.addResource(new Path(HIVE_CONF_PATH))
  implicit val timer: Timer[IO]     = IO.timer(ExecutionContext.global)
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  def multiTransactionalHiveStream(
      dbName: String,
      tblName: String,
      hconf: HiveConf
  ): Pipe[IO, Chunk[Chunk[Array[Byte]]], Stream[IO, Unit]] =
    in =>
      in.repeatPull {
        _.uncons.flatMap {
          case Some(
              (
                hd: Chunk[Chunk[Chunk[Array[Byte]]]],
                tl: Stream[IO, Chunk[Chunk[Array[Byte]]]]
              )
              ) =>
            Pull
              .output(
                hd.map { chunk =>
                  Stream
                    .resource(
                      HiveStreamingSink.create[IO](
                        dbName,
                        tblName,
                        StrictJsonWriter.newBuilder().build(),
                        hconf
                      )
                    )
                    .flatMap { hSink =>
                      val write: IO[Unit] = for {
                        statsBefore <- hSink.connectionStats()
                        _           <- hSink.writeChunks(chunk)
                        statsAfter  <- hSink.connectionStats()
                      } yield ()
                      Stream.eval(write)
                    }
                }
              )
              .as(Some(tl))
          case None => Pull.pure(None)
        }
      }

  test(
    "HiveStreamingSink Pipe with Multiple Transactions per Connection",
    IntegrationTest
  ) {

    val kStream: Stream[
      IO,
      (KafkaMetadata[Option], Either[circe.Error, FlClicks[Option, Id]])
    ] =
      consumerStream(consumerSettings)
        .evalTap(_.subscribeTo(subscribeTopic))
        .flatMap(_.stream)
        .mapAsync(1) { committable =>
          IO(
            new KafkaMetadata(
              Option(committable.record.offset),
              Option(committable.record.partition),
              Option(committable.record.topic),
              Option(committable.record.timestamp)
            ),
            decode[FlClicks[Option, Id]](committable.record.value)
          )
        }

    val flattenedStream: Stream[IO, FlClicksFlattened[Option]] = kStream
      .filter(rec => rec._2.isRight)
      .map(rec => (rec._1, rec._2.right.get))
      .balanceThrough(1000, 3)(
        Transform[
          IO,
          (KafkaMetadata[Option], FlClicks[Option, Id]),
          FlClicksKafkaEnriched[Option]
        ]
      )
      .balanceThrough(1000, 3)(
        Transform[IO, FlClicksKafkaEnriched[Option], FlClicksFlattened[Option]]
      )

    val writeTestStream: Stream[IO, Unit] = flattenedStream
      .map(s => s.asJson.toString().getBytes)
      .chunkN(10000, allowFewer = true)
      .chunkN(10, allowFewer = true)
      .through(multiTransactionalHiveStream(dbName, tblName, hiveConf))
      .parJoinUnbounded
      .handleErrorWith { rec =>
        Stream.eval(fail("Kafka Integration Test failure"))
      }

    writeTestStream.compile.drain.unsafeRunSync()

  }

}
