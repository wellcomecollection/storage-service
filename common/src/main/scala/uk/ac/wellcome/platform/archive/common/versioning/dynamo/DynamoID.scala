package uk.ac.wellcome.platform.archive.common.versioning.dynamo

import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets

import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

import scala.util.{Failure, Success, Try}

// We need to be able to look up by ExternalIdentifier and StorageSpace, both of
// which are strings from external output.
//
// To allow us to construct unique identifiers, we URL encode both strings,
// which remain readable but don't contain colons.

object DynamoID {
  def createId(storageSpace: StorageSpace,
               externalIdentifier: ExternalIdentifier): String =
    s"${encode(storageSpace.underlying)}:${encode(externalIdentifier.underlying)}"

  def getStorageSpace(id: String): Try[StorageSpace] =
    id.split(":") match {
      case Array(storageSpace, _) =>
        Success(StorageSpace(decode(storageSpace)))
      case _ =>
        Failure(
          new IllegalArgumentException(s"Malformed ID for version record: $id"))
    }

  def getExternalIdentifier(id: String): Try[ExternalIdentifier] =
    id.split(":") match {
      case Array(_, externalIdentifier) =>
        Success(ExternalIdentifier(decode(externalIdentifier)))
      case _ =>
        Failure(
          new IllegalArgumentException(s"Malformed ID for version record: $id"))
    }

  private def encode(s: String): String =
    URLEncoder.encode(s, StandardCharsets.UTF_8.toString)

  private def decode(s: String): String =
    URLDecoder.decode(s, StandardCharsets.UTF_8.toString)
}
