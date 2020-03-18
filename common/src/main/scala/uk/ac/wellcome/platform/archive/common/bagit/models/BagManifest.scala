package uk.ac.wellcome.platform.archive.common.bagit.models

import java.io.InputStream

import uk.ac.wellcome.platform.archive.common.verify.{Checksum, HashingAlgorithm, SHA256}

import scala.util.Try

case class BagManifest(
  checksumAlgorithm: HashingAlgorithm,
  files: Seq[BagFile]
)

object BagManifest {
  def create(
    inputStream: InputStream,
    algorithm: HashingAlgorithm
  ): Try[BagManifest] = {
    // Eventually calls to this method should be replaced by direct calls into
    // ManifestFileParser; we keep it here for now so we don't have to change the
    // entire codebase at once.
    assert(algorithm == SHA256)

    CombinedManifestParser
      .createFileLists(sha256 = inputStream)
      .map { fileMap =>
          val files = fileMap.map {
            case (bagPath, verifiableChecksum) =>
              BagFile(
                checksum = Checksum(
                  algorithm = algorithm,
                  value = verifiableChecksum.sha256
                ),
                path = bagPath
              )
          }.toList

          BagManifest(checksumAlgorithm = algorithm, files = files)
      }
  }
}
