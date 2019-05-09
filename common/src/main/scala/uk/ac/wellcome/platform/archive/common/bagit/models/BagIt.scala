package uk.ac.wellcome.platform.archive.common.bagit.models

import uk.ac.wellcome.platform.archive.common.storage.models.{ChecksumAlgorithm, FileManifest}
import uk.ac.wellcome.platform.archive.common.verify.{Checksum, ChecksumValue, VerifiableObjectLocation}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.platform.archive.common.storage.Resolvable._

object BagIt {
  val checksum =
    (algorithm: ChecksumAlgorithm) =>
      (file: BagDigestFile) =>
        Checksum(
          algorithm,
          ChecksumValue(file.checksum))

  val location = (root: ObjectLocation) =>
    (file: BagDigestFile) => file.resolve(root)

  val verifiable =
    (algorithm: ChecksumAlgorithm) =>
      (root: ObjectLocation) =>
        (file: BagDigestFile) =>
          VerifiableObjectLocation(
            location(root)(file),
            checksum(algorithm)(file))

  val verifyFileManifest =
    (manifest: FileManifest) =>
      (root: ObjectLocation) =>
        manifest.files.map(
          verifiable(manifest.checksumAlgorithm)(root)(_)
        )
}
