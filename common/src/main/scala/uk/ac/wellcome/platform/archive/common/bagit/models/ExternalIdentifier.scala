package uk.ac.wellcome.platform.archive.common.bagit.models

import org.scanamo.DynamoFormat
import io.circe.{Decoder, Encoder, HCursor, Json}

class ExternalIdentifier(val underlying: String) {
  override def toString: String = underlying

  require(!underlying.isEmpty, "External identifier cannot be empty")

  // Normally we use case classes for immutable data, which provide these
  // methods for us.
  //
  // We deliberately don't use case classes here so we skip automatic
  // case class derivation for JSON encoding/DynamoDB in Scanamo,
  // and force callers to intentionally import the implicits below.
  def canEqual(a: Any): Boolean = a.isInstanceOf[ExternalIdentifier]

  override def equals(that: Any): Boolean =
    that match {
      case that: ExternalIdentifier =>
        that.canEqual(this) && this.underlying == that.underlying
      case _ => false
    }
}

object ExternalIdentifier {
  def apply(underlying: String): ExternalIdentifier =
    new ExternalIdentifier(underlying)

  implicit val encoder: Encoder[ExternalIdentifier] =
    (value: ExternalIdentifier) => Json.fromString(value.toString)

  implicit val decoder: Decoder[ExternalIdentifier] = (cursor: HCursor) =>
    cursor.value.as[String].map(ExternalIdentifier(_))

  implicit def evidence: DynamoFormat[ExternalIdentifier] =
    DynamoFormat.coercedXmap[ExternalIdentifier, String, IllegalArgumentException](
      ExternalIdentifier(_)
    )(
      _.underlying
    )
}
