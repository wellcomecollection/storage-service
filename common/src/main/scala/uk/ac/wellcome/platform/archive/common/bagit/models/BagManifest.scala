package uk.ac.wellcome.platform.archive.common.bagit.models

import java.io.{BufferedReader, InputStream, InputStreamReader}

import uk.ac.wellcome.platform.archive.common.verify.{
  Checksum,
  ChecksumValue,
  HashingAlgorithm
}

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

case class BagManifest(
  checksumAlgorithm: HashingAlgorithm,
  files: List[BagFile]
)

object BagManifest {

  // Intended to match BagIt `manifest-algorithm.txt` file format:
  // https://tools.ietf.org/html/draft-kunze-bagit-17#section-2.1.3

  val LINE_REGEX: Regex = """(.+?)\s+(.+)""".r

  def create(inputStream: InputStream,
             algorithm: HashingAlgorithm): Try[BagManifest] = {

    val bufferedReader = new BufferedReader(
      new InputStreamReader(inputStream)
    )

    val lines = Iterator
      .continually(bufferedReader.readLine())
      .takeWhile { _ != null }
      .filter { _.nonEmpty }
      .toList

    val eitherFiles = lines.map(createBagFile(_, None, algorithm))

    // Collect left
    val errorStrings = eitherFiles.collect {
      case Left(errorString) => errorString
    }

    // Collect right
    val files = eitherFiles.collect {
      case Right(bagFile) => bagFile
    }

    if (errorStrings.isEmpty) {
      Success(BagManifest(algorithm, files))
    } else {
      Failure(new RuntimeException(s"Failed to parse: ${lines}"))
    }
  }

  private def createBagFile(
    line: String,
    maybeRoot: Option[String],
    algorithm: HashingAlgorithm
  ): Either[String, BagFile] = line match {
    case LINE_REGEX(checksumString, itemPathString) =>
      Right(
        BagFile(
          Checksum(
            algorithm,
            ChecksumValue.create(checksumString)
          ),
          BagPath.create(itemPathString)
        )
      )
    case l => Left(l)
  }
}
