package uk.ac.wellcome.platform.archive.common.bagit.models

import uk.ac.wellcome.platform.archive.common.storage.Resolvable._
import uk.ac.wellcome.platform.archive.common.verify.{Checksum, HashingAlgorithm, VerifiableLocation}
import uk.ac.wellcome.storage.ObjectLocation

object BagIt {
  val checksum =
    (algorithm: HashingAlgorithm) =>
      (file: BagFile) =>
        Checksum(
          algorithm,
          file.checksum)

  val location = (root: ObjectLocation) =>
    (file: BagFile) => file.resolve(root)

  val verifiable =
    (algorithm: HashingAlgorithm) =>
      (root: ObjectLocation) =>
        (file: BagFile) =>
          VerifiableLocation(
            location(root)(file),
            checksum(algorithm)(file))

  val verifyFileManifest =
    (manifest: BagManifest) =>
      (root: ObjectLocation) =>
        manifest.files.map(
          verifiable(manifest.checksumAlgorithm)(root)(_)
        )

}
