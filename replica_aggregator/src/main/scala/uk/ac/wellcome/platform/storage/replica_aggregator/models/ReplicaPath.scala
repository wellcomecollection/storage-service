package uk.ac.wellcome.platform.storage.replica_aggregator.models

import io.circe.{Decoder, Encoder, HCursor, Json}
import org.scanamo.DynamoFormat
import uk.ac.wellcome.storage.{
  AzureBlobItemLocationPrefix,
  Location,
  Prefix,
  S3ObjectLocationPrefix
}

case class ReplicaPath(underlying: String) extends AnyVal {
  override def toString: String = underlying
}

case object ReplicaPath {
  def apply(prefix: Prefix[_ <: Location]): ReplicaPath =
    prefix match {
      case S3ObjectLocationPrefix(_, keyPrefix)       => ReplicaPath(keyPrefix)
      case AzureBlobItemLocationPrefix(_, namePrefix) => ReplicaPath(namePrefix)
    }

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
