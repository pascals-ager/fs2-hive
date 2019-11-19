package io.pascals.fs2.hive.domain

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

case class Strategy (strategy: Option[String],
                     score: Option[Int],
                     decision: Option[String],
                     reason: Option[String])

object Strategy {

  implicit val StrategyDecoder: Decoder[Strategy] = deriveDecoder[Strategy]
  implicit val StrategyEncoder: Encoder[Strategy] = deriveEncoder[Strategy]
}