package weco.storage_service.bagit.models

import weco.storage_service.checksum.{ChecksumAlgorithm, ChecksumValue}

sealed trait BagManifest {
  val checksumAlgorithm: ChecksumAlgorithm
  val entries: Map[BagPath, ChecksumValue]

  def paths: Seq[BagPath] = entries.keys.toSeq
}

case class PayloadManifest(
  checksumAlgorithm: ChecksumAlgorithm,
  entries: Map[BagPath, ChecksumValue]
) extends BagManifest

case class TagManifest(
  checksumAlgorithm: ChecksumAlgorithm,
  entries: Map[BagPath, ChecksumValue]
) extends BagManifest
