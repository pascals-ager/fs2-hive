package io.pascals.fs2.hive.domain

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

case class SourceAttributes (id: Option[String],
                             origin: Option[String],
                             data: AttributesData,
                             external_data: Option[Map[String, String]])

object SourceAttributes{
  implicit val SourceAttributesDecoder: Decoder[SourceAttributes] = deriveDecoder[SourceAttributes]
  implicit val SourceAttributesEncoder: Encoder[SourceAttributes] = deriveEncoder[SourceAttributes]
}