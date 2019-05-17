package uk.ac.wellcome.platform.archive.common.bagit.models

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.URI

import scala.util.Try
import scala.util.matching.Regex

case class BagFetch(
                     files: List[BagFetchEntry]
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
              url = new URI(m.group("url")),
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

  def write(entries: Seq[BagFetchEntry]): String =
    entries
      .map { e =>
        s"${e.url} ${encodeLength(e.length)} ${encodeFilepath(e.path.value)}"
      }
      .mkString("\n")

  private def encodeLength(length: Option[Int]): String =
    length match {
      case Some(i) => i.toString
      case None    => "-"
    }

  private def decodeLength(ls: String): Option[Int] =
    if (ls == "-") None else Some(ls.toInt)

  private def encodeFilepath(path: String): String =
    path.replaceAll("\n", "%0A").replaceAll("\r", "%0D")

  private def decodeFilepath(path: String): String =
    path.replaceAll("%0A", "\n").replaceAll("%0D", "\r")
}
