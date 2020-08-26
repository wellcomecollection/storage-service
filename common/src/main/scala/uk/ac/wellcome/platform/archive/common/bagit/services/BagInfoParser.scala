package uk.ac.wellcome.platform.archive.common.bagit.services

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.time.LocalDate

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagInfo,
  ExternalDescription,
  ExternalIdentifier,
  InternalSenderDescription,
  InternalSenderIdentifier,
  PayloadOxum,
  SourceOrganisation
}

import scala.util.{Failure, Success, Try}

object BagInfoParser extends Logging {

  // Intended to match BagIt `bag-info` file format:
  // https://tools.ietf.org/html/rfc8493#section-2.2.2
  //
  // The spec notes that ordering is important and must be preserved, because this
  // is a human-readable file.  Because we are only reading a bag-info.txt and
  // not writing one, it is okay for us to ignore the ordering.

  type BagInfoMetadata = Map[String, Seq[String]]

  def create(inputStream: InputStream): Try[BagInfo] =
    for {
      bagInfoMetadata <- parse(inputStream)
      _ = debug(
        s"Successfully parsed metadata map from bag-info.txt: $bagInfoMetadata"
      )

      // Extract required fields
      externalIdentifier <- extractExternalIdentifier(bagInfoMetadata)
      _ = debug(s"Parsed External-Identifier as $externalIdentifier")

      payloadOxum <- extractPayloadOxum(bagInfoMetadata)
      _ = debug(s"Parsed Payload-Oxum as $payloadOxum")

      baggingDate <- extractBaggingDate(bagInfoMetadata)
      _ = debug(s"Parsed Bagging-Date as $baggingDate")

      // Extract optional fields
      sourceOrganisation <- extractSourceOrganisation(bagInfoMetadata)
      externalDescription <- extractExternalDescription(bagInfoMetadata)
      internalSenderIdentifier <- extractInternalSenderIdentifier(
        bagInfoMetadata
      )
      internalSenderDescription <- extractInternalSenderDescription(
        bagInfoMetadata
      )

      bagInfo = BagInfo(
        externalIdentifier = externalIdentifier,
        payloadOxum = payloadOxum,
        baggingDate = baggingDate,
        sourceOrganisation = sourceOrganisation,
        externalDescription = externalDescription,
        internalSenderIdentifier = internalSenderIdentifier,
        internalSenderDescription = internalSenderDescription
      )
    } yield bagInfo

  private def extractExternalIdentifier(
    metadata: BagInfoMetadata
  ): Try[ExternalIdentifier] =
    getSingleRequiredValue(metadata, label = "External-Identifier")
      .flatMap { value =>
        Try { ExternalIdentifier(value) } match {
          case Success(externalIdentifier) => Success(externalIdentifier)

          // The error messages from the External-Identifier apply() method are typically
          // of the form
          //
          //      requirement failed: External identifier cannot start with a slash
          //
          // That's an internal Scala detail, and not something we want to expose in an
          // end-user facing message.
          case Failure(e) =>
            Failure(
              new RuntimeException(
                s"Unable to parse External-Identifier in bag-info.txt: ${e.getMessage
                  .replaceAll("^requirement failed: ", "")}"
              )
            )
        }
      }

  // Matches the Payload-Oxum value, which is of the form "_OctetCount_._StreamCount_",
  // for example:
  //
  //      Payload-Oxum: 279164409832.1198
  //
  private val PAYLOAD_OXUM_REGEX = s"""([0-9]+)\\.([0-9]+)\\s*""".r

  private def extractPayloadOxum(metadata: BagInfoMetadata): Try[PayloadOxum] =
    getSingleRequiredValue(metadata, label = "Payload-Oxum")
      .flatMap {
        case PAYLOAD_OXUM_REGEX(payloadBytes, numberOfPayloadFiles) =>
          Success(PayloadOxum(payloadBytes.toLong, numberOfPayloadFiles.toInt))
        case line =>
          Failure(
            new RuntimeException(
              s"Unable to parse Payload-Oxum in bag-info.txt: $line"
            )
          )
      }

  private def extractBaggingDate(metadata: BagInfoMetadata): Try[LocalDate] =
    getSingleRequiredValue(metadata, label = "Bagging-Date")
      .flatMap { dateString =>
        Try { LocalDate.parse(dateString) } match {
          case Success(baggingDate) => Success(baggingDate)
          case Failure(e) =>
            Failure(
              new RuntimeException(
                s"Unable to parse Bagging-Date in bag-info.txt: ${e.getMessage}"
              )
            )
        }
      }

  private def extractSourceOrganisation(
    metadata: BagInfoMetadata
  ): Try[Option[SourceOrganisation]] =
    getSingleOptionalValue(metadata, label = "Source-Organization")
      .map { _.map(SourceOrganisation) }

  private def extractExternalDescription(
    metadata: BagInfoMetadata
  ): Try[Option[ExternalDescription]] =
    getSingleOptionalValue(metadata, label = "External-Description")
      .map { _.map(ExternalDescription) }

  private def extractInternalSenderIdentifier(
    metadata: BagInfoMetadata
  ): Try[Option[InternalSenderIdentifier]] =
    getSingleOptionalValue(metadata, label = "Internal-Sender-Identifier")
      .map { _.map(InternalSenderIdentifier) }

  private def extractInternalSenderDescription(
    metadata: BagInfoMetadata
  ): Try[Option[InternalSenderDescription]] =
    getSingleOptionalValue(metadata, label = "Internal-Sender-Description")
      .map { _.map(InternalSenderDescription) }

  private def getSingleOptionalValue(
    metadata: BagInfoMetadata,
    label: String
  ): Try[Option[String]] =
    metadata.get(label) match {
      case Some(Seq(value)) => Success(Some(value))
      case None             => Success(None)
      case Some(values) =>
        Failure(
          new RuntimeException(
            s"Multiple values for $label in bag-info.txt: ${values.mkString(", ")}"
          )
        )
    }

  private def getSingleRequiredValue(
    metadata: BagInfoMetadata,
    label: String
  ): Try[String] =
    getSingleOptionalValue(metadata, label).flatMap {
      case Some(value) => Success(value)
      case None =>
        Failure(new RuntimeException(s"Missing key in bag-info.txt: $label"))
    }

  private def parse(inputStream: InputStream): Try[BagInfoMetadata] = {
    val bufferedReader = new BufferedReader(
      new InputStreamReader(inputStream)
    )

    // Read the lines one by one, either getting a (label -> value) association,
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

    if (unparseableLines.nonEmpty) {
      Failure(
        new RuntimeException(
          s"Unable to parse the following lines in bag-info.txt: ${unparseableLines.mkString(", ")}"
        )
      )
    } else {
      Success(
        constructSummary(
          parsedLines.collect {
            case Right(entry) => entry
          }
        )
      )
    }
  }

  // It is possible for lines in the bag-info.txt to be repeated; how much we
  // care about this depends on the key in question.
  //
  // Each line is a map (label) -> (value).
  //
  // This method assembles a map (label) -> (all values) from the entire bag-info.txt.
  //
  private def constructSummary(
    parsedLines: Seq[(String, String)]
  ): BagInfoMetadata =
    parsedLines
      .foldLeft(Map[String, Seq[String]]())(
        (summary: Map[String, Seq[String]], line: (String, String)) => {
          line match {
            case (label: String, value: String) =>
              summary.updated(
                label,
                summary.getOrElse(label, Seq[String]()) ++ Seq(value)
              )
          }
        }
      )
      .map { case (label, values) => label -> values.distinct }

  // According to the BagIt spec, the format of a metadata line is:
  //
  //      A metadata element MUST consist of a label, a colon ":", a single
  //      linear whitespace character (space or tab), and a value that is
  //      terminated with an LF, a CR, or a CRLF.
  //
  // Some examples:
  //
  //      Source-Organization: FOO University
  //      Organization-Address: 1 Main St., Cupertino, California, 11111
  //      Contact-Name: Jane Doe
  //
  private val BAG_INFO_FIELD_REGEX = """([^:]+)\s*:\s(.+)\s*""".r
  private val BAG_INFO_LABEL_ONLY_REGEX = """([^:]+)\s*:\s*""".r

  private def parseSingleLine(line: String): Either[String, (String, String)] =
    line match {
      case BAG_INFO_FIELD_REGEX(label, value) => Right((label, value))
      // The BagIt spec is a little ambiguous on whether empty values are
      // allowed.  We do have bag-info.txt files with empty values in the
      // storage service (e.g. staging, digitised/b20442713/v1), so we
      // allow them.
      //
      // Some values cannot be empty, e.g. ExternalIdentifier; this is enforced
      // by the type.
      case BAG_INFO_LABEL_ONLY_REGEX(label) => Right((label, ""))
      case _                                => Left(line)
    }
}
