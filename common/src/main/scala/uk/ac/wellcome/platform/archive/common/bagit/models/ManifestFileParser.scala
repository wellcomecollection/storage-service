package uk.ac.wellcome.platform.archive.common.bagit.models
import java.io.{BufferedReader, InputStream, InputStreamReader}

import uk.ac.wellcome.platform.archive.common.verify.{ChecksumValue, VerifiableChecksum}

import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

/** Each line in a manifest file _manifest-algorithm.txt_ is of the form
  *
  *     checksum filepath
  *
  * where _checksum_ is a hex-encoded checksum for the file at _filepath_ created
  * using _algorithm_ (e.g. md5, sha256).  The payload manifest lists every
  * file in the data/ directory; the tag manifest lists every file in the
  * top directory.
  *
  * A bag can contain one or more payload/tag manifest files; if more than one, each
  * manifest file must refer to the same list of files.  This parser can read all
  * the manifest files in a bag, check the file lists are consistent, and combine
  * them together.
  *
  * See https://tools.ietf.org/html/rfc8493#section-2.1.3
  *     https://tools.ietf.org/html/rfc8493#section-2.2.1
  *
  */
object ManifestFileParser {

  // Represents a single line in a manifest file, e.g.
  //
  //    3532b221585eaa6d2520068ec1938928a1069a72fc50a8df9f3472da3f458037  data/b12345678.xml
  //
  // Note: checksums can be upper or lowercase.
  //
  protected val LINE_REGEX: Regex = new Regex("""(0-9a-fA-F+?)\s+(.+)""", "checksum", "filepath")

  def createFileLists(
    md5: Option[InputStream] = None,
    sha1: Option[InputStream] = None,
    sha256: InputStream,
    sha512: Option[InputStream] = None
  ): Try[Map[BagPath, VerifiableChecksum]] =
    for {
      filesMD5    <- maybeReadManifestFile(md5)
      filesSHA1   <- maybeReadManifestFile(sha1)
      filesSHA256 <- maybeReadManifestFile(Some(sha256))
      filesSHA512 <- maybeReadManifestFile(sha512)

      // Now check the manifest files all refer to the same list of files -- for example,
      // in case the MD5 manifest refers to {file1, file2, file3} but the SHA1 manifest
      // only refers to {file1, file2}.
      paths = List(filesMD5, filesSHA1, filesSHA256, filesSHA512)
        .flatten
        .map { _.keys.toSet }

      _ <- if (paths.distinct.size == 1) {
        Success(())
      } else {
        Failure(
          new RuntimeException("Different manifests refer to different lists of files!")
        )
      }

      // At this point, we've read the manifest files successfully, and we know each one
      // contains the same list of files.  Go ahead and combine the checksum values.
      //
      // Note: because we know each manifest contains the same list of files, we can
      // pull the key out of the map without checking if it's present (`_(bagPath)`) --
      // we know it is, or this function will already have failed.
      result =
      paths.distinct.head
        .map { bagPath =>
          bagPath -> VerifiableChecksum(
            md5 = filesMD5.map { _(bagPath) },
            sha1 = filesSHA1.map { _(bagPath) },
            sha256 = filesSHA256(bagPath),
            sha512 = filesSHA512.map { _(bagPath) }
          )
        }
        .toMap
    } yield result

  private def maybeReadManifestFile(maybeInputStream: Option[InputStream]): Try[Option[Map[BagPath, ChecksumValue]]] =
    maybeInputStream match {
      case Some(inputStream) => readSingleManifestFile(inputStream).map { Some(_) }
      case None              => Success(None)
    }

  // Parses a single manifest file.
  private def readSingleManifestFile(inputStream: InputStream): Try[Map[BagPath, ChecksumValue]] = {
    val bufferedReader = new BufferedReader(
      new InputStreamReader(inputStream)
    )

    // Read the lines one by one, either getting a (filepath -> checksum) association,
    // or returning the line if it doesn't parse correctly.
    val lines = Iterator
      .continually(bufferedReader.readLine())
      .takeWhile { _ != null }
      .filter { _.nonEmpty }
      .toList

    val parsedLines = lines.map { parseSingleLine }

    val unparseableLines = parsedLines.collect {
      case Left(line) => line
    }

    // The BagIt spec says a manifest cannot refer to the same file more than once;
    // for example, this is illegal (even though the checksums match):
    //
    //    abcdef  myfile.txt
    //    abcdef  myfile.txt
    //
    // This looks for duplicate paths in the manifest file.
    val paths = parsedLines
      .collect { case Right(assoc) => assoc }
      .flatMap { _.keys }

    val duplicatePaths: List[BagPath] = paths
      .groupBy { identity }
      .mapValues { _.size }
      .filter { case (_, count) => count > 1 }
      .map { case (bagPath, _) => bagPath }
      .toList

    // NOTE: If a manifest contains the same file path twice but with two different
    // checksums, this will only keep the last one.  Strictly speaking, it's an
    // error for a manifest file to refer to the same file twice, and we should
    // fail parsing here if that occurs.
    val associations: Map[BagPath, ChecksumValue] = parsedLines
      .collect { case Right(assoc) => assoc }
      .reduce { _ ++ _ }

    if (unparseableLines.nonEmpty) {
      Failure(new RuntimeException(s"Failed to parse the following lines: $unparseableLines"))
    } else if (duplicatePaths.nonEmpty) {
      Failure(new RuntimeException(s"Manifest contains duplicate paths: $duplicatePaths"))
    } else {
      Success(associations)
    }
  }

  private def parseSingleLine(line: String): Either[String, Map[BagPath, ChecksumValue]] = line match {
    case LINE_REGEX(checksum, filepath) =>
      Right(Map(BagPath.create(filepath) -> ChecksumValue.create(checksum)))
    case _ => Left(line)
  }
}
