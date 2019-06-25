package uk.ac.wellcome.platform.archive.common.versioning.dynamo

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.Base64

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.versioning.VersionRecord

import scala.util.{Failure, Success, Try}

// We need to be able to look up by ExternalIdentifier and StorageSpace, both of
// which are strings from external output.
//
// To allow us to construct unique identifiers, we encode both strings with
// base64 (which doesn't include a colon).

object DynamoID {
  def createId(storageSpace: StorageSpace, externalIdentifier: ExternalIdentifier): String =
    s"${toBase64(storageSpace.underlying)}:${toBase64(externalIdentifier.underlying)}"

  def getStorageSpace(id: String): Try[StorageSpace] =
    id.split(":") match {
      case Array(storageSpace, _) => Success(StorageSpace(fromBase64(storageSpace)))
      case _ => Failure(new IllegalArgumentException(s"Malformed ID for version record: $id"))
    }

  def getExternalIdentifier(id: String): Try[ExternalIdentifier] =
    id.split(":") match {
      case Array(_, externalIdentifier) => Success(ExternalIdentifier(fromBase64(externalIdentifier)))
      case _ => Failure(new IllegalArgumentException(s"Malformed ID for version record: $id"))
    }

  private def toBase64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  private def fromBase64(s: String): String =
    new String(Base64.getDecoder.decode(s), StandardCharsets.UTF_8)
}

case class DynamoEntry(
  id: String,
  ingestId: IngestID,
  ingestDate: Instant,
  version: Int
) {
  def toVersionRecord: Try[VersionRecord] =
    for {
      storageSpace <- DynamoID.getStorageSpace(id)
      externalIdentifier <- DynamoID.getExternalIdentifier(id)
      record = VersionRecord(
        externalIdentifier = externalIdentifier,
        ingestId = ingestId,
        ingestDate = ingestDate,
        storageSpace = storageSpace,
        version = version
      )
    } yield record
}

case object DynamoEntry {
  def apply(versionRecord: VersionRecord): DynamoEntry =
    DynamoEntry(
      id = DynamoID.createId(
        storageSpace = versionRecord.storageSpace,
        externalIdentifier = versionRecord.externalIdentifier
      ),
      ingestId = versionRecord.ingestId,
      ingestDate = versionRecord.ingestDate,
      version = versionRecord.version
    )
}
