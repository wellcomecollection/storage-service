package uk.ac.wellcome.platform.archive.common.bagit

import java.nio.file.Paths

import uk.ac.wellcome.platform.archive.common.bagit.models.BagIt.verifyFileManifest
import uk.ac.wellcome.platform.archive.common.storage.Resolvable
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.ObjectLocation
import scala.language.implicitConversions

package object models {
  import Resolvable._

  // resolvers

  implicit val bagPathResolver: Resolvable[BagPath] = new Resolvable[BagPath] {
    override def resolve(root: ObjectLocation)(
      bagPath: BagPath): ObjectLocation = {
      val paths = Paths.get(root.key, bagPath.value)
      root.copy(key = paths.toString)
    }
  }

  implicit val bagFileResolver: Resolvable[BagFile] = new Resolvable[BagFile] {
    override def resolve(root: ObjectLocation)(
      bagFile: BagFile): ObjectLocation = {
      bagFile.path.resolve(root)
    }
  }

  implicit val bagManifest: Resolvable[BagManifest] =
    new Resolvable[BagManifest] {
      override def resolve(root: ObjectLocation)(
        bag: BagManifest): ObjectLocation = {
        root
      }
    }

  implicit val bagResolver: Resolvable[Bag] = new Resolvable[Bag] {
    override def resolve(root: ObjectLocation)(bag: Bag): ObjectLocation = {
      root
    }
  }

  // verifiables

  implicit class ResolvedVerifiable[T](t: T)(
    implicit
    resolver: Resolvable[T],
    f: ObjectLocation => T => List[VerifiableLocation]
  ) {
    def verifiable(
      root: ObjectLocation
    ) = {

      new Verifiable[T] {
        override def create(t: T): List[VerifiableLocation] = {
          val resolved = resolver.resolve(root)(t)

          f(resolved)(t)
        }
      }
    }
  }

  // ObjectLocation => T => List[VerifiableLocation]

  implicit def bagManifestVerifiable(root: ObjectLocation)(
    manifest: BagManifest): List[VerifiableLocation] =
    verifyFileManifest(manifest)(root)

  implicit def bagVerifiable(root: ObjectLocation)(bag: Bag) = {
    bagManifestVerifiable(root)(bag.manifest) ++ bagManifestVerifiable(root)(
      bag.tagManifest)
  }

}
