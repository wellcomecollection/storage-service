package weco.storage_service.bagit.models

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.{URI, URISyntaxException}
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

case class BagFetchMetadata(
  uri: URI,
  length: Option[Long]
)

case class BagFetch(
  entries: Map[BagPath, BagFetchMetadata]
) {
  def paths: Seq[BagPath] = entries.keys.toSeq
}

object BagFetch {

  /** Read/write the contents of a Fetch File as defined by RFC 8493 § 2.2.3.
    * See https://tools.ietf.org/html/rfc8493#section-2.2.3
    *
    * Relevant notes:
    *
    *   - Each line of a fetch file MUST be of the form
    *
    *         url length filepath
    *
    *     `url` must be an absolute URI, and whitespace characters must be
    *     percent encoded
    *     `length` is the number of octets in the file, or "-" if unspecified
    *     `filename` is the path to the file.  Line break characters (LR, CF, LRCF)
    *     and *only* those characters must be percent-encoded.
    *
    *   - A fetch file must not list any tag files (everything in the fetch file
    *     must be in the payload; that is, in the data/ directory).
    *
    */
  val FETCH_LINE_REGEX: Regex = new Regex(
    "(.*)[ \t]+(\\d*|-)[ \t]+(.*)",
    "url",
    "length",
    "filepath"
  )

  def create(stream: InputStream): Try[BagFetch] = Try {
    val bufferedReader = new BufferedReader(new InputStreamReader(stream))

    val lines: List[String] =
      Iterator
        .continually(bufferedReader.readLine())
        .takeWhile { _ != null }
        .filterNot { _.trim.isEmpty }
        .toList

    val entries = lines
      .zipWithIndex
      .map { case (line, lineNo) =>
        // Ensure line numbers are 1-indexed
        (line, lineNo + 1)
      }
      .map { case (line, lineNo) =>
        FETCH_LINE_REGEX.findFirstMatchIn(line) match {
          case Some(m) =>
            val path = BagPath(decodeFilepath(m.group("filepath")))

            val metadata = BagFetchMetadata(
              uri = decodeUri(line, lineNo, m.group("url")),
              length = decodeLength(m.group("length"))
            )

            path -> metadata
          case None =>
            throw new RuntimeException(
              s"Line <<$line>> is incorrectly formatted!"
            )
        }
      }

    // The BagIt spec says the fetch.txt must not list any tag files; that is, metadata
    // files in the top-level of the bag.  It must only contain payload files.
    val tagFilesInFetch = entries.filterNot {
      case (path, _) => path.value.startsWith("data/")
    }

    if (tagFilesInFetch.nonEmpty) {
      val pathList =
        tagFilesInFetch.map { case (path, _) => path.value }.mkString(", ")
      throw new RuntimeException(
        s"fetch.txt should not contain tag files: $pathList"
      )
    }

    // Although the BagIt spec doesn't say this explicitly, it seems reasonable to expect
    // that each bag path is listed *at most once* in a fetch.txt.  If a path is listed
    // multiple times, it is ambiguous where we should get the file from, so throw an error.
    val duplicatePaths =
      entries
        .map { case (path, _) => path }
        .groupBy { identity }
        .mapValues { _.size }
        .filter { case (_, count) => count > 1 }
        .collect { case (bagPath, _) => bagPath.value }
        .toList
        .sorted

    if (duplicatePaths.nonEmpty) {
      throw new RuntimeException(
        s"fetch.txt contains duplicate paths: ${duplicatePaths.mkString(", ")}"
      )
    }

    BagFetch(entries.toMap)
  }

  private def decodeUri(line: String, lineNo: Int, u: String): URI =
    Try { new URI(u) } match {
      case Failure(_: URISyntaxException) if u.contains(" ") =>
        throw new RuntimeException(s"URI is incorrectly formatted on line $lineNo. Spaces should be URL-encoded: $line")

      case Failure(e: URISyntaxException) =>
        val wrappedExc = new URISyntaxException(line, e.getReason, e.getIndex)
        throw new Throwable(s"URI is incorrectly formatted on line $lineNo. ${wrappedExc.getMessage}")

      case Success(uri) => uri
      case Failure(err) => throw err
    }

  private def decodeLength(ls: String): Option[Long] =
    if (ls == "-") None else Some(ls.toLong)

  // Quoting RFC 8493 § 2.2.3 (https://datatracker.ietf.org/doc/html/rfc8493#section-2.2.3):
  //
  //      If _filename_ includes an LF, a CR, a CRLF, or a percent sign (%), those
  //      characters (and only those) MUST be percent-encoded as described in [RFC3986].
  //
  private def decodeFilepath(path: String): String =
    path
      .replaceAll("%0A", "\n")
      .replaceAll("%0D", "\r")
      .replaceAll("%25", "%")
}
