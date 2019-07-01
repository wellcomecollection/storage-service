package uk.ac.wellcome.platform.archive.display.fixtures

import java.time.format.DateTimeFormatter

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagFile,
  BagInfo,
  BagManifest
}
import uk.ac.wellcome.platform.archive.common.ingests.models.StorageLocation

trait DisplayJsonHelpers {
  private def stringField[T](t: T): String =
    s"""
       |"${t.toString}"
   """.stripMargin

  def bagInfo(info: BagInfo): String =
    s"""
       |{
       |  "externalIdentifier": "${info.externalIdentifier}",
       |  ${optionalField(
         "externalDescription",
         stringField,
         info.externalDescription)}
       |  ${optionalField(
         "internalSenderIdentifier",
         stringField,
         info.internalSenderIdentifier)}
       |  ${optionalField(
         "internalSenderDescription",
         stringField,
         info.internalSenderDescription)}
       |  ${optionalField(
         "sourceOrganization",
         stringField,
         info.sourceOrganisation)}
       |  "payloadOxum": "${info.payloadOxum.toString}",
       |  "baggingDate": "${info.baggingDate.format(
         DateTimeFormatter.ISO_LOCAL_DATE)}",
       |  "type": "BagInfo"
       |}
     """.stripMargin

  def location(loc: StorageLocation): String =
    s"""
       |{
       |  "provider": {
       |    "id": "${loc.provider.id}",
       |    "type": "Provider"
       |  },
       |  "bucket": "${loc.location.namespace}",
       |  "path": "${loc.location.path}",
       |  "type": "Location"
       |}
     """.stripMargin

  private def bagDigestFile(file: BagFile): String =
    s"""
       |{
       |  "type": "File",
       |  "path": "${file.path}",
       |  "checksum": "${file.checksum.value}"
       |}
     """.stripMargin

  def manifest(fm: BagManifest): String =
    s"""
       |{
       |  "type": "BagManifest",
       |  "checksumAlgorithm": "${fm.checksumAlgorithm.value}",
       |  "files": [ ${asList(fm.files, bagDigestFile)} ]
       |}
     """.stripMargin

  def asList[T](elements: Seq[T], formatter: T => String): String =
    elements.map { formatter(_) }.mkString(",")

  def optionalField[T](fieldName: String,
                       formatter: T => String,
                       maybeObjectValue: Option[T],
                       lastField: Boolean = false): String =
    maybeObjectValue match {
      case None => ""
      case Some(o) =>
        s"""
           "$fieldName": ${formatter(o)}${if (!lastField) ","}
         """
    }
}
