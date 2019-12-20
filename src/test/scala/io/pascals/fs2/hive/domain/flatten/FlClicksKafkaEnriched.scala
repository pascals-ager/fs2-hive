package io.pascals.fs2.hive.domain.flatten

import cats.Id
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import io.pascals.fs2.hive.domain.kafka.KafkaMetadata

case class FlClicksKafkaEnriched[F[_]]( click: FlClicks[Option, Id],
                                        meta: F[KafkaMetadata[Option]] )

object FlClicksKafkaEnriched {

  implicit val OwClicksKafkaEnrichedDecoder: Decoder[FlClicksKafkaEnriched[Option]] = deriveDecoder[FlClicksKafkaEnriched[Option]]
  implicit val OwClicksKafkaEnrichedEncoder: Encoder[FlClicksKafkaEnriched[Option]] = deriveEncoder[FlClicksKafkaEnriched[Option]]
}