package uk.ac.wellcome.platform.archive.common.bagit

import java.io.FileNotFoundException

import scala.util.Try

/** Sometimes we get told "there's a bag at this location" but it's not at
  * the root.  We need to find the root of the bag, which we can detect
  * by the presence of a "/bag-info.txt" file.
  */
object BagInfoLocator {
  def locateBagInfo(filenames: Iterator[String]): Try[String] = Try {
    val matching =
      filenames
        .filter { f: String =>
          f
            .split("/")
            .last
            .endsWith("bag-info.txt")
        }
        .toSeq

    matching match {
      case Seq(filename) => filename
      case Seq() => throw new FileNotFoundException("No bag-info.txt file found!")
      case _ => throw new IllegalArgumentException(
        s"Multiple bag-info.txt files found, only wanted one: ${matching.mkString(", ")}"
      )
    }
  }
}
