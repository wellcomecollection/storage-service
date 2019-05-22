package uk.ac.wellcome.platform.archive.common.bagit.services

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.bagit.models.Bag
import uk.ac.wellcome.platform.archive.common.bagit.{models, MatchedLocation}
import uk.ac.wellcome.platform.archive.common.storage.{Locatable, Resolvable}
import uk.ac.wellcome.platform.archive.common.verify.{
  Verifiable,
  VerifiableGenerationFailed,
  VerifiableGenerationFailure,
  VerifiableLocation
}
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success}

class BagVerifiable(root: ObjectLocation)(
  implicit resolvable: Resolvable[ObjectLocation]
) extends Verifiable[Bag]
    with Logging {

  import Locatable._
  import Resolvable._
  import models._

  protected def matchBagLocation(
    bagFiles: List[BagFile],
    fetchEntries: List[BagFetchEntry]
  ): Either[List[Throwable], List[MatchedLocation]] = {

    val filtered = bagFiles.map { file =>
      val matches = fetchEntries.collect {
        case entry if file.path == entry.path =>
          MatchedLocation(file, Some(entry))
      }

      if (matches.isEmpty) List(MatchedLocation(file, None)) else matches
    }

    val matched = filtered.map {
      case List(matched @ MatchedLocation(_, _)) =>
        Success(Some(matched))

      case Nil => Success(None)

      case _ =>
        Failure(
          new RuntimeException(
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

  private def getVerifiableLocation(
    matched: MatchedLocation): Either[Throwable, VerifiableLocation] =
    matched match {
      case MatchedLocation(bagFile: BagFile, Some(fetchEntry)) =>
        Right(VerifiableLocation(fetchEntry.uri, bagFile.checksum))

      case MatchedLocation(bagFile: BagFile, None) =>
        bagFile.locateWith(root) match {
          case Left(e) => Left(VerifiableGenerationFailed(e.msg))
          case Right(location) =>
            Right(
              VerifiableLocation(
                location.resolve,
                bagFile.checksum
              ))
        }
    }

  private def combine(errors: List[Throwable]) =
    VerifiableGenerationFailed(errors.map(_.getMessage).mkString("\n"))

  override def create(
    bag: Bag): Either[VerifiableGenerationFailure, List[VerifiableLocation]] = {
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

        val failures = matches collect { case Left(f)           => f }
        val successes = matches collect { case Right(locations) => locations }

        debug(s"Got ($successes, $failures)")

        Either.cond(failures.isEmpty, successes, combine(failures))
    }
  }
}
