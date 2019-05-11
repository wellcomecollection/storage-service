package uk.ac.wellcome.platform.archive.common.bagit

import java.nio.file.Paths

import uk.ac.wellcome.platform.archive.common.bagit.models.BagIt.verifyFileManifest
import uk.ac.wellcome.platform.archive.common.storage.Resolvable
import uk.ac.wellcome.platform.archive.common.storage.models.{ChecksumAlgorithm, FileManifest}
import uk.ac.wellcome.platform.archive.common.verify.{Checksum, VerifiableLocation}
import uk.ac.wellcome.storage.ObjectLocation


package object models {
  import Resolvable._

  // resolvers

  // TODO: Can I resolve a bag here?

  implicit val bagItemPathResolver = new Resolvable[BagItemPath] {
    override def resolve(root: ObjectLocation)(path: BagItemPath): ObjectLocation = {
      val paths = Paths.get(root.key, path.value)
      root.copy(key = paths.toString)
    }
  }

  implicit val bagDigestFileResolver = new Resolvable[BagDigestFile] {
    override def resolve(root: ObjectLocation)(bag: BagDigestFile): ObjectLocation = {
      bag.path.resolve(root)
    }
  }

  // verifiables

  // ObjectLocation => ChecksumAlgorithm => T => List[VerifiableLocation]

  implicit def bagDigestFileVerifiable(root: ObjectLocation)(algorithm: ChecksumAlgorithm)(file: BagDigestFile): List[VerifiableLocation] = List(VerifiableLocation(
        file.path.resolve(root),
        Checksum(algorithm, file.checksum)))

  implicit def fileManifestVerifiable(root: ObjectLocation)(algorithm: ChecksumAlgorithm)(
    manifest: FileManifest
  ): List[VerifiableLocation] = verifyFileManifest(manifest)(root)

  implicit def bagVerifiable(root: ObjectLocation)(algorithm: ChecksumAlgorithm)(bag: Bag): List[VerifiableLocation] =
    List(bag.manifest, bag.tagManifest)
      .map(verifyFileManifest)
      .flatMap(withRoot => withRoot(root))

}