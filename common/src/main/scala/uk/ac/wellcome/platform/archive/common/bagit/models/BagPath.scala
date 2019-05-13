package uk.ac.wellcome.platform.archive.common.bagit.models

import com.gu.scanamo.DynamoFormat
import com.gu.scanamo.error.TypeCoercionError
import io.circe.{Decoder, Encoder, Json}
import uk.ac.wellcome.json.JsonUtil.{fromJson, toJson}

case class BagPath(value: String)

object BagPath {
  def apply(
    itemPath: String,
    maybeBagRootPath: Option[String] = None
  ): BagPath = {

    maybeBagRootPath match {
      case None => BagPath(itemPath)
      case Some(bagRootPath) =>
        BagPath(f"${rTrimPath(bagRootPath)}/${lTrimPath(itemPath)}")
    }
  }

  private def lTrimPath(path: String): String = {
    path.replaceAll("^/", "")
  }

  private def rTrimPath(path: String): String = {
    path.replaceAll("/$", "")
  }

  implicit val encoder: Encoder[BagPath] = Encoder.instance[BagPath] {
    space: BagPath =>
      Json.fromString(space.toString)
  }

  implicit val decoder: Decoder[BagPath] =
    Decoder.instance[BagPath](cursor =>
      cursor.value.as[String].map(BagPath(_)))

  implicit def fmtSpace: DynamoFormat[BagPath] =
    DynamoFormat.xmap[BagPath, String](
      fromJson[BagPath](_)(decoder).toEither.left
        .map(TypeCoercionError)
    )(
      toJson[BagPath](_).get
    )
}
