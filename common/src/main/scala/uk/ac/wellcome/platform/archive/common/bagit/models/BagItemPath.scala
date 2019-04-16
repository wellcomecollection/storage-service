package uk.ac.wellcome.platform.archive.common.bagit.models

import java.nio.file.Paths

import com.gu.scanamo.DynamoFormat
import com.gu.scanamo.error.TypeCoercionError
import io.circe.{Decoder, Encoder, Json}
import uk.ac.wellcome.json.JsonUtil.{fromJson, toJson}
import uk.ac.wellcome.storage.ObjectLocation

case class BagItemPath(underlying: String) extends AnyVal {
  override def toString: String = underlying

  def toObjectLocation(bagRootLocation: ObjectLocation): ObjectLocation =
    bagRootLocation.copy(
      key = Paths
        .get(
          bagRootLocation.key,
          underlying
        )
        .toString)
}

object BagItemPath {
  def apply(
    itemPath: String,
    maybeBagRootPath: Option[String] = None
  ): BagItemPath = {

    maybeBagRootPath match {
      case None => BagItemPath(itemPath)
      case Some(bagRootPath) =>
        BagItemPath(f"${rTrimPath(bagRootPath)}/${lTrimPath(itemPath)}")
    }
  }

  private def lTrimPath(path: String): String = {
    path.replaceAll("^/", "")
  }

  private def rTrimPath(path: String): String = {
    path.replaceAll("/$", "")
  }

  implicit val encoder: Encoder[BagItemPath] = Encoder.instance[BagItemPath] {
    space: BagItemPath =>
      Json.fromString(space.toString)
  }

  implicit val decoder: Decoder[BagItemPath] =
    Decoder.instance[BagItemPath](cursor =>
      cursor.value.as[String].map(BagItemPath(_)))

  implicit def fmtSpace: DynamoFormat[BagItemPath] =
    DynamoFormat.xmap[BagItemPath, String](
      fromJson[BagItemPath](_)(decoder).toEither.left
        .map(TypeCoercionError)
    )(
      toJson[BagItemPath](_).get
    )
}
