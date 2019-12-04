package uk.ac.wellcome.platform.archive.common.storage.models

import org.scanamo.DynamoFormat
import io.circe.{Decoder, Encoder, HCursor, Json}

class StorageSpace(val underlying: String) {
  override def toString: String = underlying

  require(!underlying.isEmpty, "Storage space cannot be empty")

  // Normally we use case classes for immutable data, which provide these
  // methods for us.
  //
  // We deliberately don't use case classes here so we skip automatic
  // case class derivation for JSON encoding/DynamoDB in Scanamo,
  // and force callers to intentionally import the implicits below.
  def canEqual(a: Any): Boolean = a.isInstanceOf[StorageSpace]

  override def equals(that: Any): Boolean =
    that match {
      case that: StorageSpace =>
        that.canEqual(this) && this.underlying == that.underlying
      case _ => false
    }

  // At various points in the pipeline, we combine the storage space and
  // the external identifier into a bag ID, for example:
  //
  //    space = "digitised"
  //    identifier = "b12345678"
  //     => bag ID = "digitised/b12345678"
  //
  // We allow slashes in the IDs (for example, to match CALM IDs like PP/MIA/1).
  // To avoid ambiguity when looking at a bag ID, we forbid slashes in
  // the external identifier.
  //
  // For example, this allows us to say unambiguously that
  //
  //    bag ID = "alfa/bravo/charlie"
  //     => space = "alfa"
  //        identifier = "bravo/charlie"
  //
  // If both the space and external identifier could contain slashes,
  // this bag ID would be ambiguous.
  require(
    !underlying.contains("/"),
    s"Storage space cannot contain slashes, but got $underlying"
  )
}

object StorageSpace {
  def apply(underlying: String): StorageSpace =
    new StorageSpace(underlying)

  implicit val encoder: Encoder[StorageSpace] = (value: StorageSpace) =>
    Json.fromString(value.toString)

  implicit val decoder: Decoder[StorageSpace] = (cursor: HCursor) =>
    cursor.value.as[String].map(StorageSpace(_))

  implicit def evidence: DynamoFormat[StorageSpace] =
    DynamoFormat.coercedXmap[StorageSpace, String, IllegalArgumentException](
      StorageSpace(_)
    )(
      _.underlying
    )
}
