package uk.ac.wellcome.platform.archive.common.bagit.services

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.bagit.models._
import uk.ac.wellcome.platform.archive.common.storage.{Locatable, Resolvable}
import uk.ac.wellcome.platform.archive.common.verify.{
  Verifiable,
  VerifiableGenerationFailed,
  VerifiableGenerationFailure,
  VerifiableLocation
}
import uk.ac.wellcome.storage.ObjectLocation

class BagVerifiable(root: ObjectLocation)(
  implicit resolvable: Resolvable[ObjectLocation]
) extends Verifiable[Bag]
    with Logging {

  import Locatable._

  override def create(
    bag: Bag): Either[VerifiableGenerationFailure, Seq[VerifiableLocation]] = {
    debug(s"Attempting to create Seq[VerifiableLocation] for $bag")

    BagMatcher.correlateFetchEntries(bag) match {
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

  private def getVerifiableLocation(
    matched: MatchedLocation): Either[Throwable, VerifiableLocation] =
    matched match {
      case MatchedLocation(bagFile: BagFile, Some(fetchEntry)) =>
        Right(
          VerifiableLocation(
            uri = fetchEntry.uri,
            checksum = bagFile.checksum,
            length = fetchEntry.length
          ))

      case MatchedLocation(bagFile: BagFile, None) =>
        bagFile.locateWith(root) match {
          case Left(e) => Left(VerifiableGenerationFailed(e.msg))
          case Right(location) =>
            Right(
              VerifiableLocation(
                uri = resolvable.resolve(location),
                checksum = bagFile.checksum,
                length = None
              ))
        }
    }

  private def combine(errors: Seq[Throwable]) =
    VerifiableGenerationFailed(errors.map(_.getMessage).mkString("\n"))
}
