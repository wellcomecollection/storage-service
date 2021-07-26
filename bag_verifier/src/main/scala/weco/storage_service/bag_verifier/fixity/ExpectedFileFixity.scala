package weco.storage_service.bag_verifier.fixity

import weco.storage_service.bagit.models.BagPath
import weco.storage_service.checksum.MultiManifestChecksum

import java.net.URI

sealed trait ExpectedFileFixity {
  val uri: URI
  val path: BagPath
  val multiChecksum: MultiManifestChecksum
  val length: Option[Long]
}

case class FetchFileFixity(
  uri: URI,
  path: BagPath,
  multiChecksum: MultiManifestChecksum,
  length: Option[Long]
) extends ExpectedFileFixity

case class DataDirectoryFileFixity(
  uri: URI,
  path: BagPath,
  multiChecksum: MultiManifestChecksum
) extends ExpectedFileFixity {
  val length: Option[Long] = None
}
