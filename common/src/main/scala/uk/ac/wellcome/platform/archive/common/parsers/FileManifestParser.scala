package uk.ac.wellcome.platform.archive.common.parsers

import java.io.InputStream

import uk.ac.wellcome.platform.archive.common.models.{ChecksumAlgorithm, FileManifest}
import uk.ac.wellcome.platform.archive.common.models.bagit.{BagDigestFile, BagItemPath}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}


object FileManifestParser {
  val checksumLineRegex: Regex = """(.+?)\s+(.+)""".r

  def create(
              inputStream: InputStream,
              checksumAlgorithm: ChecksumAlgorithm
            )(implicit executionContext: ExecutionContext): Future[FileManifest] = {

    val lines = scala.io.Source
      .fromInputStream(inputStream)
      .mkString
      .split("\n")
      .filter { _.nonEmpty }
      .toList

    val tryBagDigestFiles = lines.map { line =>
      createBagDigestFile(
        line = line,
        bagRootPathInZip = None
      )
    }

    val futureDigestFiles = Future.sequence(
      tryBagDigestFiles.map { Future.fromTry }
    )

    futureDigestFiles.map(files =>
      FileManifest(
        checksumAlgorithm,
        files
      )
    )
  }

  private def createBagDigestFile(
    line: String,
    bagRootPathInZip: Option[String]
  ): Try[BagDigestFile] = line match {
      case checksumLineRegex(checksum, itemPath) =>
        Success(
          BagDigestFile(
            checksum = checksum.trim,
            path = BagItemPath(itemPath.trim, bagRootPathInZip)
          )
        )
      case _ =>
        Failure(new RuntimeException(
          s"Line <<$line>> is incorrectly formatted!")
        )
    }
}
