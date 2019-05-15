package uk.ac.wellcome.platform.archive.common.bagit.models

import java.io.InputStream

import uk.ac.wellcome.platform.archive.common.verify.{ChecksumValue, HashingAlgorithm}

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

case class BagManifest(
                        checksumAlgorithm: HashingAlgorithm,
                        files: List[BagFile]
                      )

object BagManifest {
  val lineRegex: Regex = """(.+?)\s+(.+)""".r

  def create(stream: InputStream, algorithm: HashingAlgorithm): Try[BagManifest] = {

    val lines = scala.io.Source
      .fromInputStream(stream)
      .mkString
      .split("\n")
      .filter { _.nonEmpty }
      .toList

    val eitherFiles = lines.zipWithIndex.map {
      case (l, i)  => createBagFile(i, l, None)
    }

    val errorStrings = eitherFiles.collect { case Left(errorString) => errorString }
    val files = eitherFiles.collect { case Right(bagFile) => bagFile }

    if(errorStrings.isEmpty) {
      Success(BagManifest(algorithm, files))
    } else {
      Failure(new RuntimeException(s"Failed to parse: ${lines}"))
    }
  }

  private def createBagFile(
    lineNumber: Int,
    line: String,
    maybeRoot: Option[String]
  ): Either[String, BagFile] = line match {
    case lineRegex(checksum, itemPath) => Right(BagFile(
        ChecksumValue(checksum.trim),
        BagPath(itemPath.trim, maybeRoot)))
    case line => Left(s"$lineNumber: $line")
  }
}
