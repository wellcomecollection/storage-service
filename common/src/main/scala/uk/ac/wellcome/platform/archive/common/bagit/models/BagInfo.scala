package uk.ac.wellcome.platform.archive.common.bagit.models

import java.io.InputStream
import java.time.LocalDate

import cats.data.ValidatedNel
import cats.implicits._

import scala.util.Try

case class BagInfo(
  externalIdentifier: ExternalIdentifier,
  payloadOxum: PayloadOxum,
  baggingDate: LocalDate,
  sourceOrganisation: Option[SourceOrganisation] = None,
  externalDescription: Option[ExternalDescription] = None,
  internalSenderIdentifier: Option[InternalSenderIdentifier] = None,
  internalSenderDescription: Option[InternalSenderDescription] = None
)

object BagInfo {

  // Intended to match BagIt `bag-info` file format:
  // https://tools.ietf.org/html/rfc8493#section-2.2.2
  //
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
  // The spec notes that ordering is important and must be preserved, because this
  // is a human-readable file.  Because we are only reading a bag-info.txt and
  // not writing one, it is okay for us to ignore the ordering.

  private val BAG_INFO_FIELD_REGEX = """(.*?)\s*:\s*(.*)\s*""".r
  private val PAYLOAD_OXUM_REGEX =
    s"""${BagInfoKeys.payloadOxum}\\s*:\\s*([0-9]+)\\.([0-9]+)\\s*""".r

  def create(inputStream: InputStream): Try[BagInfo] = {
    validate(inputStream).toEither.left
      .map(e => new RuntimeException(e.toString))
      .toTry
  }

  private def validate(inputStream: InputStream): ValidatedNel[String, BagInfo] = {
    val bagInfoLines = scala.io.Source
      .fromInputStream(inputStream, "UTF-8")
      .mkString
      .split("\n")

    val validated: ValidatedNel[String, BagInfo] = (
      extractExternalIdentifier(bagInfoLines),
      extractPayloadOxum(bagInfoLines),
      extractBaggingDate(bagInfoLines),
      extractSourceOrganisation(bagInfoLines),
      extractExternalDescription(bagInfoLines),
      extractInternalSenderIdentifier(bagInfoLines),
      extractInternalSenderDescription(bagInfoLines)
    ).mapN(
      (
        externalIdentifier: ExternalIdentifier,
        payloadOxum: PayloadOxum,
        baggingDate: LocalDate,
        sourceOrganisation: Option[SourceOrganisation],
        externalDescription: Option[ExternalDescription],
        internalSenderIdentifier: Option[InternalSenderIdentifier],
        internalSenderDescription: Option[InternalSenderDescription]
      ) =>
        BagInfo.apply(
          externalIdentifier,
          payloadOxum,
          baggingDate,
          sourceOrganisation,
          externalDescription,
          internalSenderIdentifier,
          internalSenderDescription
        )
    )

    validated
  }

  private def extractExternalIdentifier(bagInfoLines: Array[String]) =
    extractRequiredValue(bagInfoLines, BagInfoKeys.externalIdentifier)
      .map(ExternalIdentifier.apply)

  private def extractPayloadOxum(bagInfoLines: Array[String]) =
    bagInfoLines
      .collectFirst {
        case PAYLOAD_OXUM_REGEX(bytes, numberOfFiles) =>
          PayloadOxum(bytes.toLong, numberOfFiles.toInt)
      }
      .toValidNel(BagInfoKeys.payloadOxum)

  private def extractBaggingDate(bagInfoLines: Array[String]) =
    extractRequiredValue(bagInfoLines, BagInfoKeys.baggingDate).andThen(
      dateString =>
        Try(LocalDate.parse(dateString)).toEither
          .leftMap(_ => BagInfoKeys.baggingDate)
          .toValidatedNel
    )

  private def extractSourceOrganisation(bagInfoLines: Array[String]) =
    extractOptionalValue(bagInfoLines, BagInfoKeys.sourceOrganisation)
      .map(SourceOrganisation)
      .validNel

  private def extractExternalDescription(bagInfoLines: Array[String]) =
    extractOptionalValue(bagInfoLines, BagInfoKeys.externalDescription)
      .map(ExternalDescription)
      .validNel

  private def extractInternalSenderIdentifier(bagInfoLines: Array[String]) =
    extractOptionalValue(bagInfoLines, BagInfoKeys.internalSenderIdentifier)
      .map(InternalSenderIdentifier)
      .validNel

  private def extractInternalSenderDescription(bagInfoLines: Array[String]) =
    extractOptionalValue(bagInfoLines, BagInfoKeys.internalSenderDescription)
      .map(InternalSenderDescription)
      .validNel

  private def extractRequiredValue(
    bagInfoLines: Array[String],
    bagInfoKey: String
  ) =
    extractOptionalValue(bagInfoLines, bagInfoKey)
      .toValidNel(bagInfoKey)

  private def extractOptionalValue(
    bagInfoLines: Array[String],
    bagInfoKey: String
  ) =
    bagInfoLines
      .collectFirst {
        case BAG_INFO_FIELD_REGEX(key, value) if key == bagInfoKey =>
          value
      }
}

object BagInfoKeys {
  val externalIdentifier = "External-Identifier"
  val baggingDate = "Bagging-Date"
  val payloadOxum = "Payload-Oxum"
  val sourceOrganisation = "Source-Organization"
  val externalDescription = "External-Description"
  val internalSenderIdentifier = "Internal-Sender-Identifier"
  val internalSenderDescription = "Internal-Sender-Description"
}

case class InternalSenderIdentifier(underlying: String) extends AnyVal {
  override def toString: String = underlying
}
case class InternalSenderDescription(underlying: String) extends AnyVal {
  override def toString: String = underlying
}
case class ExternalDescription(underlying: String) extends AnyVal {
  override def toString: String = underlying
}
case class SourceOrganisation(underlying: String) extends AnyVal {
  override def toString: String = underlying
}
case class PayloadOxum(payloadBytes: Long, numberOfPayloadFiles: Int) {
  override def toString = s"$payloadBytes.$numberOfPayloadFiles"
}
