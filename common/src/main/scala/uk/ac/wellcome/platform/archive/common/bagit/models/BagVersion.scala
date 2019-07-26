package uk.ac.wellcome.platform.archive.common.bagit.models

import io.circe.{Decoder, Encoder, HCursor, Json}
import org.scanamo.DynamoFormat

case class BagVersion(underlying: Int) extends AnyVal {
  override def toString: String = s"v$underlying"

  def increment: BagVersion =
    BagVersion(underlying + 1)
}

object BagVersion {
  implicit val encoder: Encoder[BagVersion] =
    (version: BagVersion) => Json.fromInt(version.underlying)

  implicit val decoder: Decoder[BagVersion] = (cursor: HCursor) =>
    cursor.value.as[Int].map { BagVersion(_) }

  implicit def evidence: DynamoFormat[BagVersion] =
    DynamoFormat.iso[BagVersion, Int](
      BagVersion(_)
    )(
      _.underlying
    )

  implicit val ordering: Ordering[BagVersion] =
    (x: BagVersion, y: BagVersion) => Ordering[Int].compare(x.underlying, y.underlying)
}

