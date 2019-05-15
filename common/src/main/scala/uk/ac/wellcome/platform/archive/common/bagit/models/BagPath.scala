package uk.ac.wellcome.platform.archive.common.bagit.models

import com.gu.scanamo.DynamoFormat
import com.gu.scanamo.error.TypeCoercionError
import io.circe.{Decoder, Encoder, Json}
import uk.ac.wellcome.json.JsonUtil._

case class BagPath(value: String) {
  override def toString: String = value
}

object BagPath {
  def create(raw: String) =
    BagPath(raw.trim)

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

  implicit val enc = Encoder.instance[BagPath](o =>
    Json.fromString(o.toString))

  implicit val dec = Decoder.instance[BagPath](cursor =>
    cursor.value.as[String].map(BagPath(_)))

  implicit def fmt =
    DynamoFormat.xmap[BagPath, String](
      fromJson[BagPath](_)(dec).toEither.left
        .map(TypeCoercionError)
    )(
      toJson[BagPath](_).get
    )
}
