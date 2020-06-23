package uk.ac.wellcome.platform.archive.bagverifier.fixity.bag

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.bagverifier.fixity.{
  CannotCreateExpectedFixity,
  ExpectedFileFixity,
  ExpectedFixity
}
import uk.ac.wellcome.platform.archive.bagverifier.storage.{Locatable, Resolvable}
import uk.ac.wellcome.platform.archive.bagverifier.storage.bag.BagLocatable._
import uk.ac.wellcome.platform.archive.common.bagit.models._
import uk.ac.wellcome.platform.archive.common.bagit.services.BagMatcher
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.ObjectLocation

class BagExpectedFixity(root: ObjectLocation)(
  implicit resolvable: Resolvable[ObjectLocation]
) extends ExpectedFixity[Bag]
    with Logging {

  import Locatable._

  override def create(
    bag: Bag
  ): Either[CannotCreateExpectedFixity, Seq[ExpectedFileFixity]] = {
    debug(s"Attempting to get the fixity info for $bag")

    BagMatcher.correlateFetchEntries(bag) match {
      case Left(error) =>
        debug(s"Left: $error")
        Left(combine(Seq(error)))
      case Right(matched) =>
        debug(s"Right: $matched")

        val matches = matched.map(getVerifiableLocation)

        val failures = matches collect { case Left(f)           => f }
        val successes = matches collect { case Right(locations) => locations }

        debug(s"Got ($successes, $failures)")

        Either.cond(failures.isEmpty, successes, combine(failures))
    }
  }

  private def getVerifiableLocation(
    matched: MatchedLocation
  ): Either[Throwable, ExpectedFileFixity] =
    matched match {
      case MatchedLocation(
          bagPath: BagPath,
          checksum: Checksum,
          Some(fetchEntry)
          ) =>
        Right(
          ExpectedFileFixity(
            uri = fetchEntry.uri,
            path = bagPath,
            checksum = checksum,
            length = fetchEntry.length
          )
        )

      case MatchedLocation(bagPath: BagPath, checksum: Checksum, None) =>
        bagPath.locateWith(root) match {
          case Left(e) => Left(CannotCreateExpectedFixity(e.msg))
          case Right(location) =>
            Right(
              ExpectedFileFixity(
                uri = resolvable.resolve(location),
                path = bagPath,
                checksum = checksum,
                length = None
              )
            )
        }
    }

  private def combine(errors: Seq[Throwable]) =
    CannotCreateExpectedFixity(errors.map(_.getMessage).mkString("\n"))
}
