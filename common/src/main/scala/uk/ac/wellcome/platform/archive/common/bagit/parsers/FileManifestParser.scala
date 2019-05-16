package uk.ac.wellcome.platform.archive.common.bagit.parsers

import java.io.InputStream

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagDigestFile,
  BagItemPath
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  ChecksumAlgorithm,
  FileManifest
}

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

object FileManifestParser {
  val checksumLineRegex: Regex = """(.+?)\s+(.+)""".r

  def create(
    inputStream: InputStream,
    checksumAlgorithm: ChecksumAlgorithm
  ): Try[FileManifest] = {

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

    val errors = tryBagDigestFiles.collect {
      case Failure(error) => error
    }

    val files = tryBagDigestFiles.collect {
      case Success(bagFile) => bagFile
    }

    if (errors.isEmpty) {
      Success(FileManifest(checksumAlgorithm, files))
    } else {
      Failure(new RuntimeException(s"Failed to parse: $errors"))
    }
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
      Failure(new RuntimeException(s"Line <<$line>> is incorrectly formatted!"))
  }
}
