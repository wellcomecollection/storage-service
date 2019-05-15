package uk.ac.wellcome.platform.archive.common.bagit.models

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
}
