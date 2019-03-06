package uk.ac.wellcome.platform.archive.archivist.bag

import uk.ac.wellcome.platform.archive.archivist.models.errors.InvalidBagManifestError
import uk.ac.wellcome.platform.archive.common.models.bagit.{
  BagDigestFile,
  BagItemPath
}
import uk.ac.wellcome.platform.archive.common.models.error.ArchiveError

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

object BagDigestFileCreator {

  val checksumLineRegex: Regex = """(.+?)\s+(.+)""".r

  /** Within a manifest, each entry in the bag is a single line, containing
    * a checksum and the location.  For example:
    *
    *     676...8c32  data/b12345678.xml
    *     593...5c5  data/alto/b12345678_0001.xml
    *     26a...78c  data/alto/b12345678_0002.xml
    *
    * Given a single line, this method extracts the checksum and the
    * location, or returns an error if the line is incorrectly formatted.
    *
    */
  def create(line: String,
             bagRootPathInZip: Option[String],
             manifestName: String): Try[BagDigestFile] =
    line match {
      case checksumLineRegex(checksum, itemPath) =>
        Success(
          BagDigestFile(
            checksum = checksum.trim,
            path = BagItemPath(itemPath.trim, bagRootPathInZip)
          )
        )
      case _ =>
        Failure(
          new RuntimeException(s"Line <<$line>> is incorrectly formatted!"))
    }

  def create[T](line: String,
                job: T,
                bagRootPathInZip: Option[String] = None,
                manifestName: String): Either[ArchiveError[T], BagDigestFile] =
    create(line, bagRootPathInZip, manifestName) match {
      case Success(bagDigestFile) => Right(bagDigestFile)
      case Failure(_)             => Left(InvalidBagManifestError(job, manifestName, line))
    }
}
