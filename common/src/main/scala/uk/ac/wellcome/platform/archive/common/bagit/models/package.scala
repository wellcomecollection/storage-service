package uk.ac.wellcome.platform.archive.common.bagit

import java.nio.file.Paths

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.bagit.models.{Bag, BagFetchEntry, BagFile}
import uk.ac.wellcome.platform.archive.common.storage.{Locatable, LocateFailure, LocationNotFound}
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success}


case class MatchedLocation(bagFile: BagFile, b: Option[BagFetchEntry])


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
}

class BagVerifiable(root: ObjectLocation) extends Verifiable[Bag] with Logging {
  import Locatable._
  import models._

  protected def matchBagLocation(
                                  bagFiles: List[BagFile],
                                  fetchEntries: List[BagFetchEntry]
  ): Either[List[Throwable], List[MatchedLocation]] = {

    val filtered = bagFiles.map { file =>

      val matches = fetchEntries.collect { case entry if file.path == entry.path =>
        MatchedLocation(file, Some(entry))
      }

      if(matches.isEmpty) List(MatchedLocation(file, None)) else matches
    }

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

    val failures = matched
      .collect { case Failure(e: Throwable) => e }

    Either.cond(failures.isEmpty, successes, failures)
  }

  private def getVerifiableLocation(matched: MatchedLocation): Either[Throwable, VerifiableLocation] =
    matched match {
      case MatchedLocation(bagFile: BagFile, Some(_)) => {
        Right(VerifiableLocation(ObjectLocation("namespace", "key"), bagFile.checksum))
      }
      case MatchedLocation(bagFile: BagFile, None) =>
        bagFile.locateWith(root) match {
          case Left(e) => Left(VerifiableGenerationFailed(e.msg))
          case Right(location) => Right(VerifiableLocation(location, bagFile.checksum))
        }
    }

  private def combine(errors: List[Throwable]) =
    VerifiableGenerationFailed(errors.map(_.getMessage).mkString("\n"))

  override def create(bag: Bag): Either[VerifiableGenerationFailure, List[VerifiableLocation]] = {
    debug(s"Attempting to create List[VerifiableLocation] for $bag")

    val bagFiles = bag.tagManifest.files ++ bag.manifest.files
    val fetchEntries = bag.fetch.toList.flatMap(_.files)

    debug(s"List[BagFile]: $bagFiles")
    debug(s"List[BagFetchEntries]: $fetchEntries")

    matchBagLocation(bagFiles, fetchEntries) match {
      case Left(errors) =>
        debug(s"Left: $errors")
        Left(combine(errors))
      case Right(matched) =>
        debug(s"Right: $matched")

        val matches = matched.map(getVerifiableLocation)

        val failures = matches collect { case Left(f) => f }
        val successes = matches collect { case Right(locations) => locations }

        debug(s"Got ($successes, $failures)")

        Either.cond(failures.isEmpty, successes, combine(failures))
    }
  }
}