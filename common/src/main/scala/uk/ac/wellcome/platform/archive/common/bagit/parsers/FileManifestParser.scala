package uk.ac.wellcome.platform.archive.common.bagit.parsers

import java.io.InputStream

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagDigestFile, BagItemPath}
import uk.ac.wellcome.platform.archive.common.storage.models.{ChecksumAlgorithm, FileManifest}
import uk.ac.wellcome.platform.archive.common.verify.ChecksumValue

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

    futureDigestFiles.map(
      files =>
        FileManifest(
          checksumAlgorithm,
          files
      ))
  }

  private def createBagDigestFile(
    line: String,
    bagRootPathInZip: Option[String]
  ): Try[BagDigestFile] = line match {
    case checksumLineRegex(checksum, itemPath) =>
      Success(
        BagDigestFile(
          ChecksumValue(checksum.trim),
          BagItemPath(itemPath.trim, bagRootPathInZip)
        )
      )
    case _ =>
      Failure(new RuntimeException(s"Line <<$line>> is incorrectly formatted!"))
  }
}
