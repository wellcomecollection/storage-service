package weco.storage_service.bag_verifier.fixity

import weco.storage_service.bagit.models.BagPath
import weco.storage_service.checksum.{Checksum, MultiManifestChecksum}

import java.net.URI

sealed trait ExpectedFileFixity {
  val uri: URI
  val path: BagPath
  val multiChecksum: MultiManifestChecksum
  val length: Option[Long]

  def checksum: Checksum = {
    val algorithm = multiChecksum.definedAlgorithms.head

    Checksum(
      algorithm = algorithm,
      value = multiChecksum.getValue(algorithm).get
    )
  }
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
