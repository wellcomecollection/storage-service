package uk.ac.wellcome.platform.storage.replica_aggregator.models

import io.circe.{Decoder, Encoder, HCursor, Json}
import org.scanamo.DynamoFormat

case class ReplicaPath(underlying: String) extends AnyVal {
  override def toString: String = underlying
}

object ReplicaPath {
  implicit val encoder: Encoder[ReplicaPath] =
    (value: ReplicaPath) => Json.fromString(value.toString)

  implicit val decoder: Decoder[ReplicaPath] = (cursor: HCursor) =>
    cursor.value.as[String].map(ReplicaPath(_))

  implicit def evidence: DynamoFormat[ReplicaPath] =
    DynamoFormat.iso[ReplicaPath, String](
      ReplicaPath(_)
    )(
      _.underlying
    )
}