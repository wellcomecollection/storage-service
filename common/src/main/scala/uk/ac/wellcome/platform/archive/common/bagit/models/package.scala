package uk.ac.wellcome.platform.archive.common.bagit

import java.nio.file.Paths

import uk.ac.wellcome.platform.archive.common.bagit.models.BagLocation
import uk.ac.wellcome.platform.archive.common.storage.{Locatable, LocateFailure, LocationNotFound}
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.ObjectLocation

//import scala.language.implicitConversions
import scala.util.{Failure, Success}


case class MatchedLocation[A <: BagLocation, B <: BagLocation](a: A, b: Option[B])


package object models {
  // locatable

  private def locateBagPath(root: ObjectLocation)(bagPath: BagPath) = {
    val paths = Paths.get(root.key, bagPath.value)
    root.copy(key = paths.toString)
  }

  implicit val bagPathLocator: Locatable[BagPath] = new Locatable[BagPath] {
    override def locate(bagPath: BagPath)(maybeRoot: Option[ObjectLocation]): Either[LocateFailure[BagPath], ObjectLocation] = {
      maybeRoot match {
        case None => Left(LocationNotFound(bagPath, s"No root specified!"))
        case Some(root) => Right(locateBagPath(root)(bagPath))
      }
    }
  }

  implicit val bagFileLocator: Locatable[BagFile] = new Locatable[BagFile] {
    override def locate(bagFile: BagFile)(maybeRoot: Option[ObjectLocation]): Either[LocateFailure[BagFile], ObjectLocation] = {
      maybeRoot match {
        case None => Left(LocationNotFound(bagFile, s"No root specified!"))
        case Some(root) => Right(locateBagPath(root)(bagFile.path))
      }
    }
  }

  // verifiable

  implicit val bagVerifier: Verifiable[Bag] = new Verifiable[Bag] {
    private def matchBagLocation[A <: BagLocation, B <: BagLocation](
      a: List[A],
      b: List[B]
    ) = {

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

      val successes = matched
        .collect { case Success(t) => t }
        .collect { case Some(o) => o }

      val failures = matched.collect { case Failure(e: Throwable) => e }

      if(failures.isEmpty) Right(successes) else Left(failures)
    }

    override def create(bag: Bag): List[VerifiableLocation] = {
      val bagFiles = bag.tagManifest.files ++ bag.manifest.files
      val fetchEntries = bag.fetch.toList.flatMap(_.files)


      matchBagLocation(bagFiles, fetchEntries) match {
        case Left(_) => List.empty[VerifiableLocation]
        case Right(matchedLocations) => matchedLocations map {
          case MatchedLocation(bagFile, Some(fetchEntry: BagLocation)) => {

            VerifiableLocation(ObjectLocation("namespace", "key"), bagFile.checksum)

          }
          case MatchedLocation(bagFile, None) =>
            VerifiableLocation(ObjectLocation("namespace", "key"), bagFile.checksum)
        }
      }
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
