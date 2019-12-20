package io.pascals.fs2.hive.domain.flatten

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

case class FlStrategy(strategy: String,
                      score: Int,
                      decision: String,
                      reason: String)

object FlStrategy {

  implicit val OwStrategyDecoder: Decoder[FlStrategy] = deriveDecoder[FlStrategy]
  implicit val OwStrategyEncoder: Encoder[FlStrategy] = deriveEncoder[FlStrategy]
}
