package uk.ac.wellcome.platform.archive.common.bagit

import java.nio.file.Paths

import uk.ac.wellcome.platform.archive.common.storage.Resolvable
import uk.ac.wellcome.platform.archive.common.verify.VerifiableObjectLocation
import uk.ac.wellcome.storage.ObjectLocation


package object models {
  import Resolvable._

  // resolvers

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

  implicit def verifiableBagIt(bag: Bag): Seq[VerifiableObjectLocation] = {
    import BagIt._

    val root = ObjectLocation("","")

    List(bag.manifest, bag.tagManifest)
      .map(verifyFileManifest)
      .flatMap(withRoot => withRoot(root))
  }
}
