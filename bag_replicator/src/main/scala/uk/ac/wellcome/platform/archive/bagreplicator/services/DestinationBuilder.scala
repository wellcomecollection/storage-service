package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.nio.file.Paths

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocationPrefix

class DestinationBuilder(namespace: String) {
  def buildDestination(
    storageSpace: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: BagVersion
  ): ObjectLocationPrefix = ObjectLocationPrefix(
    namespace = namespace,
    path = Paths
      .get(
        encode(storageSpace.toString),
        encode(externalIdentifier.toString),
        version.toString
      )
      .toString
  )

  // We encode the storage space and external identifier with
  // a URL encoder so that:
  //
  //  - the path created is still human-readable
  //  - slashes are escaped
  //
  // This means the replicator will never mix up:
  //
  //        space = "a/b"   identifier = "c"
  // and
  //        space = "a"     identifier = "b/c"
  //
  // They will get distinct paths.
  //
  // Names that only contain alphanumeric characters and hyphens
  // (e.g. b1234, digitised-workflow) will be unmodified.
  //
  private def encode(s: String): String =
    URLEncoder.encode(s, StandardCharsets.UTF_8.toString)
}
