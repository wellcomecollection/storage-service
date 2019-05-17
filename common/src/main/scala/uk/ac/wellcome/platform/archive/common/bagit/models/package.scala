package uk.ac.wellcome.platform.archive.common.bagit

import java.nio.file.Paths

import uk.ac.wellcome.platform.archive.common.storage.Resolvable
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.ObjectLocation

import scala.language.implicitConversions
import scala.util.{Failure, Success}


package object models {
  import Resolvable._

  // resolvers

  implicit val bagPathResolver: Resolvable[BagPath] = new Resolvable[BagPath] {
    override def resolve(root: ObjectLocation)(bagPath: BagPath): ObjectLocation = {
      val paths = Paths.get(root.key, bagPath.value)
      root.copy(key = paths.toString)
    }
  }

  implicit val bagFileResolver: Resolvable[BagFile] = new Resolvable[BagFile] {
    override def resolve(root: ObjectLocation)(bagFile: BagFile): ObjectLocation = {
      bagFile.path.resolve(root)
    }
  }

  // verifiables

  case class MatchedLocation[A <: BagLocation, B <: BagLocation](a: A, b: Option[B])

  implicit val bagVerifier: Verifiable[Bag] = new Verifiable[Bag] {
    private def matchBagLocation[A <: BagLocation, B <: BagLocation](
      a: List[A],
      b: List[B]
    ): Either[List[Throwable], List[MatchedLocation[A, B]]] = {

      val filtered = a.map { l1 => b.collect {
        case l2 if l1.path == l2.path => MatchedLocation(l1, Some(l2))
        case _ => MatchedLocation(l1, None)
      }}

      val matched = filtered.map {
        case List(matched@MatchedLocation(_,_)) =>
          Success(Some(matched))

        case Nil => Success(None)

        case _ => Failure(new RuntimeException(
          "Found multiple matches for fetch!"
        ))
      }

      val successes = matched.collect { case Success(s) => s } flatten
      val failures = matched.collect { case Failure(e: Throwable) => e } flatten

      if(failures.isEmpty) Right(successes) else Left(failures)
    }

    override def create(bag: Bag): List[VerifiableLocation] = {
      val bagFiles = bag.tagManifest.files ++ bag.manifest.files
      val fetchEntries = bag.fetch.toList.flatMap(_.files)



      for {
        matched <- matchBagLocation(bagFiles, fetchEntries)


      }
      matchBagLocation(bagFiles, fetchEntries).map(_.map {
        case MatchedLocation(bagFile: BagFile, fetchEntry: BagFetchEntry) => {

        }
      })

      Nil
    }
  }

//  val checksum =
//    (algorithm: HashingAlgorithm) =>
//      (file: BagFile) =>
//        Checksum(
//          algorithm,
//          file.checksum)
//
//  val location = (root: ObjectLocation) =>
//    (file: BagFile) => file.resolve(root)
//
//  val verifiable =
//    (algorithm: HashingAlgorithm) =>
//      (root: ObjectLocation) =>
//        (file: BagFile) =>
//          VerifiableLocation(
//            location(root)(file),
//            checksum(algorithm)(file))
//
//  val verifyFileManifest =
//    (manifest: BagManifest) =>
//      (root: ObjectLocation) =>
//        manifest.files.map(
//          verifiable(manifest.checksumAlgorithm)(root)(_)
//        )
}
