package uk.ac.wellcome.platform.archive.common.bagit.parsers

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.URI

import scala.util.matching.Regex

case class FetchEntry(
  url: URI,
  length: Option[Int],
  filepath: String
)

object FetchReader {
  val FETCH_LINE_REGEX: Regex = ".*[ \t]+(\\d*|-)[ \t]+.*".r

  def read(inputStream: InputStream): Seq[FetchEntry] = {
    val bufferedReader = new BufferedReader(new InputStreamReader(inputStream))

    Iterator
      .continually(bufferedReader.readLine())
      .foreach { line: String =>
        println(FETCH_LINE_REGEX.findFirstMatchIn(line))
      }

    Seq.empty
  }
}

object FetchContents {

  /** Create the contents of a Fetch File as defined by RFC 8493 § 2.2.3.
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
  def write(entries: Seq[FetchEntry]): String =
    entries.map { e =>
      s"${e.url} ${e.length.getOrElse("-")} ${encodeFilepath(e.filepath)}"
    }.mkString("\n")

  private def encodeFilepath(path: String): String =
    path.replaceAll("\n", "%0A").replaceAll("\r", "%0D")
}
