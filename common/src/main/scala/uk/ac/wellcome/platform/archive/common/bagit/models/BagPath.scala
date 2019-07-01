package uk.ac.wellcome.platform.archive.common.bagit.models

import io.circe.{Decoder, Encoder, HCursor, Json}
import org.scanamo.DynamoFormat

case class BagPath(value: String) {
  override def toString: String = value
}

object BagPath {
  def create(raw: String) =
    BagPath(raw.trim)

  def apply(
    itemPath: String,
    maybeBagRootPath: Option[String] = None
  ): BagPath =
    maybeBagRootPath match {
      case None => BagPath(itemPath)
      case Some(bagRootPath) =>
        BagPath(f"${rTrimPath(bagRootPath)}/${lTrimPath(itemPath)}")
    }

  private def lTrimPath(path: String): String =
    path.replaceAll("^/", "")

  private def rTrimPath(path: String): String =
    path.replaceAll("/$", "")

  implicit val encoder: Encoder[BagPath] = (value: BagPath) =>
    Json.fromString(value.toString)

  implicit val decoder: Decoder[BagPath] = (cursor: HCursor) =>
    cursor.value.as[String].map(BagPath(_))

  implicit def evidence: DynamoFormat[BagPath] =
    DynamoFormat.iso[BagPath, String](
      BagPath(_)
    )(
      _.toString
    )
}
