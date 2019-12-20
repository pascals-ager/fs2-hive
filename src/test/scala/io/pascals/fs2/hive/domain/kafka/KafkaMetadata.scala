package io.pascals.fs2.hive.domain.kafka

import java.time.{Instant, OffsetDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import fs2.kafka.Timestamp
import io.circe.Decoder.Result
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder, HCursor, Json}

case class KafkaMetadata[F[_]]( offset: F[Long], partition: F[Int], topic: F[String], ts: F[Timestamp] )


object KafkaMetadata {

  implicit val KafkaTimeFormat: Encoder[Timestamp] with Decoder[Timestamp] = new Encoder[Timestamp] with Decoder[Timestamp] {
    override def apply( a: Timestamp ): Json = Encoder.encodeString.apply(OffsetDateTime.ofInstant(Instant.ofEpochMilli(a.createTime.getOrElse(0L)), ZoneId.of("CET")).toString)

    override def apply( c: HCursor ): Result[Timestamp] = Decoder.decodeString.map(s => Timestamp.createTime(OffsetDateTime.parse(s, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toEpochSecond)).apply(c)
  }
  implicit val KafkaMetadataDecoder: Decoder[KafkaMetadata[Option]] = deriveDecoder[KafkaMetadata[Option]]
  implicit val KafkaMetadataEncoder: Encoder[KafkaMetadata[Option]] = deriveEncoder[KafkaMetadata[Option]]
}
