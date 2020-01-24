package io.pascals.fs2.hive.domain

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

case class SourceAttributesWithArray(
    id: Option[String],
    origin: Option[String],
    data: AttributesDataWithArray,
    external_data: Option[Map[String, String]]
)

object SourceAttributesWithArray {
  implicit val SourceAttributesWithArrayDecoder
      : Decoder[SourceAttributesWithArray] =
    deriveDecoder[SourceAttributesWithArray]
  implicit val SourceAttributesWithArrayEncoder
      : Encoder[SourceAttributesWithArray] =
    deriveEncoder[SourceAttributesWithArray]
}
