package uk.ac.wellcome.platform.archive.common.bagit.models

import java.io.InputStream

import uk.ac.wellcome.platform.archive.common.bagit.services.BagManifestParser
import uk.ac.wellcome.platform.archive.common.verify.{ChecksumValue, HashingAlgorithm}

import scala.util.Try

sealed trait BagManifest {
  val checksumAlgorithm: HashingAlgorithm
  val entries: Map[BagPath, ChecksumValue]

  def paths: Seq[BagPath] = entries.keys.toSeq
}

case class PayloadManifest(
  checksumAlgorithm: HashingAlgorithm,
  entries: Map[BagPath, ChecksumValue]
) extends BagManifest

case object PayloadManifest {
  def create(
    inputStream: InputStream,
    checksumAlgorithm: HashingAlgorithm
  ): Try[PayloadManifest] =
    BagManifestParser.parse(inputStream).map { entries =>
      PayloadManifest(
        checksumAlgorithm = checksumAlgorithm,
        entries = entries
      )
    }
}

case class TagManifest(
  checksumAlgorithm: HashingAlgorithm,
  entries: Map[BagPath, ChecksumValue]
) extends BagManifest

case object TagManifest {
  def create(
    inputStream: InputStream,
    checksumAlgorithm: HashingAlgorithm
  ): Try[TagManifest] =
    BagManifestParser.parse(inputStream).map { entries =>
      TagManifest(
        checksumAlgorithm = checksumAlgorithm,
        entries = entries
      )
    }
}

