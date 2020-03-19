package uk.ac.wellcome.platform.archive.common.bagit.models

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.URI

import scala.util.Try
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
      .map { line: String =>
        FETCH_LINE_REGEX.findFirstMatchIn(line) match {
          case Some(m) =>
            val path = BagPath(decodeFilepath(m.group("filepath")))
            val metadata = BagFetchMetadata(
              uri = new URI(m.group("url")),
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
    val tagFilesInFetch = entries.filterNot { case (path, _) => path.value.startsWith("data/") }

    if (tagFilesInFetch.nonEmpty) {
      val pathList = tagFilesInFetch.map { case (path, _) => path.value }.mkString(", ")
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

  private def decodeLength(ls: String): Option[Long] =
    if (ls == "-") None else Some(ls.toLong)

  private def decodeFilepath(path: String): String =
    path.replaceAll("%0A", "\n").replaceAll("%0D", "\r")
}
