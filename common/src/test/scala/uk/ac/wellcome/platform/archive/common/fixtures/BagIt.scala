package uk.ac.wellcome.platform.archive.common.fixtures

import java.time.LocalDate

import uk.ac.wellcome.platform.archive.common.bagit.models._
import uk.ac.wellcome.platform.archive.common.generators.BagInfoGenerators

trait BagIt extends BagInfoGenerators {
  def bagInfoFileContents(
    bagIdentifier: ExternalIdentifier,
    payloadOxum: PayloadOxum,
    baggingDate: LocalDate,
    sourceOrganisation: Option[SourceOrganisation] = None,
    externalDescription: Option[ExternalDescription] = None,
    internalSenderIdentifier: Option[InternalSenderIdentifier] = None,
    internalSenderDescription: Option[InternalSenderDescription] = None)
    : String = {
    def optionalLine[T](maybeValue: Option[T], fieldName: String) =
      maybeValue.map(value => s"$fieldName: $value").getOrElse("")

    val sourceOrganisationLine =
      optionalLine(sourceOrganisation, "Source-Organization")
    val descriptionLine =
      optionalLine(externalDescription, "External-Description")
    val internalSenderIdentifierLine =
      optionalLine(internalSenderIdentifier, "Internal-Sender-Identifier")
    val internalSenderDescriptionLine =
      optionalLine(internalSenderDescription, "Internal-Sender-Description")

    s"""External-Identifier: $bagIdentifier
       |Payload-Oxum: ${payloadOxum.payloadBytes}.${payloadOxum.numberOfPayloadFiles}
       |Bagging-Date: ${baggingDate.toString}
       |$sourceOrganisationLine
       |$descriptionLine
       |$internalSenderIdentifierLine
       |$internalSenderDescriptionLine
      """.stripMargin.trim
  }
}
