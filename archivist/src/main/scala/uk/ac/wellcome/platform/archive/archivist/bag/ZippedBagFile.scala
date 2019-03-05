package uk.ac.wellcome.platform.archive.archivist.bag

import java.util.zip.ZipFile

import uk.ac.wellcome.platform.archive.common.bagit.BagInfoLocator

import scala.collection.JavaConverters._
import scala.util.Try

object ZippedBagFile {
  def bagPathFromBagInfoPath(bagInfo: String): Option[String] =
    BagInfoLocator.bagPathFrom(bagInfoPath = bagInfo) match {
      case ""        => None
      case s: String => Some(s)
    }

  def locateBagInfo(zipFile: ZipFile): Try[String] = {
    val entries =
      zipFile
        .entries()
        .asScala
        .filterNot { _.isDirectory }
        .map { _.getName }

    BagInfoLocator.locateBagInfo(entries)
  }
}
