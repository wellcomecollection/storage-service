package uk.ac.wellcome.platform.archive.common.bagit.models

import weco.json.{TypedString, TypedStringOps}

class BagPath(val value: String) extends TypedString[BagPath] {
  val underlying: String = value
}

object BagPath extends TypedStringOps[BagPath] {
  override def apply(underlying: String): BagPath = new BagPath(underlying)

  def create(raw: String): BagPath =
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
}
