package uk.ac.wellcome.platform.archive.common.bagit.models

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.URI

import scala.util.Try
import scala.util.matching.Regex

case class BagFetch(
  files: Seq[BagFetchEntry]
)

object BagFetch {

  /** Read/write the contents of a Fetch File as defined by RFC 8493 § 2.2.3.
    *
    * Relevant notes:
    *
    *   - Each line of a fetch file MUST be of the form
    *
    *         url length filepath
    *
    *   - `url` must be an absolute URI, and whitespace characters must be
    *     percent encoded
    *   - `length` is the number of octets in the file, or "-" if unspecified
    *   - `filename` is the path to the file.  Line break characters (LR, CF, LRCF)
    *     and *only* those characters must be percent-encoded.
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
            BagFetchEntry(
              uri = new URI(m.group("url")),
              length = decodeLength(m.group("length")),
              path = BagPath(decodeFilepath(m.group("filepath")))
            )
          case None =>
            throw new RuntimeException(
              s"Line <<$line>> is incorrectly formatted!"
            )
        }

      }

    BagFetch(entries)
  }

  private def decodeLength(ls: String): Option[Long] =
    if (ls == "-") None else Some(ls.toLong)

  private def decodeFilepath(path: String): String =
    path.replaceAll("%0A", "\n").replaceAll("%0D", "\r")
}
