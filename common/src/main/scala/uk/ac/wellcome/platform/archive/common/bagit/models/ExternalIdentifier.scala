package uk.ac.wellcome.platform.archive.common.bagit.models

import org.scanamo.DynamoFormat
import io.circe.{Decoder, Encoder, HCursor, Json}

class ExternalIdentifier(val underlying: String) {
  override def toString: String = underlying

  require(!underlying.isEmpty, "External identifier cannot be empty")

  // When you want to see all versions of a bag in the bags API, you call
  //
  //    GET /bags/{space}/{externalIdentifier}/versions
  //
  // To avoid ambiguity, block callers from creating an identifier that
  // ends with /versions.
  require(
    !underlying.endsWith("/versions"),
    "External identifier cannot end with /versions"
  )

  // When we store a bag in S3, we store different versions of it under the key
  //
  //    s3://{bucket}/{space}/{externalIdentifier}/v1
  //                                              /v2
  //                                              /v3
  //
  // To avoid confusion when browsing S3, block callers from creating an
  // identifier that includes anything that looks like /v1, /v2, etc.
  require(
    !underlying.matches("^.*/v\\d+$"),
    "External identifier cannot end with a version string"
  )

  require(
    !underlying.matches("^.*/v\\d+/.*$"),
    "External identifier cannot contain a version string"
  )

  require(
    !underlying.matches("^v\\d+/.*$"),
    "External identifier cannot start with a version string"
  )

  // If you put a slash at the end of the identifier (e.g. "b12345678/"), you'd
  // get an S3 key like:
  //
  //    s3://{bucket}/{space}/b12345678//v1
  //
  // The S3 Console is liable to do weird things if you have a double slash in
  // the key, so prevent people from putting slashes at the beginning or end.
  require(
    !underlying.startsWith("/"),
    "External identifier cannot start with a slash"
  )
  require(
    !underlying.endsWith("/"),
    "External identifier cannot end with a slash"
  )
  require(
    !underlying.contains("//"),
    "External identifier cannot contain consecutive slashes"
  )

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

  override def hashCode: Int = underlying.hashCode
}

object ExternalIdentifier {
  def apply(underlying: String): ExternalIdentifier =
    new ExternalIdentifier(underlying)

  implicit val encoder: Encoder[ExternalIdentifier] =
    (value: ExternalIdentifier) => Json.fromString(value.toString)

  implicit val decoder: Decoder[ExternalIdentifier] = (cursor: HCursor) =>
    cursor.value.as[String].map(ExternalIdentifier(_))

  implicit def evidence: DynamoFormat[ExternalIdentifier] =
    DynamoFormat
      .coercedXmap[ExternalIdentifier, String, IllegalArgumentException](
        ExternalIdentifier(_)
      )(
        _.underlying
      )
}
