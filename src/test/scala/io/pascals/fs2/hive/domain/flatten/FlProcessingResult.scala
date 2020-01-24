package io.pascals.fs2.hive.domain.flatten

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

case class FlProcessingResult[F[_]](
    score: F[Int],
    fingerprint: F[String],
    redirect_url: F[String],
    token: F[String],
    strategies: F[List[FlStrategy]],
    decision: F[String]
)

object FlProcessingResult {

  implicit val ProcessingResultDecoder: Decoder[FlProcessingResult[Option]] =
    deriveDecoder[FlProcessingResult[Option]]
  implicit val ProcessingResultEncoder: Encoder[FlProcessingResult[Option]] =
    deriveEncoder[FlProcessingResult[Option]]
}
