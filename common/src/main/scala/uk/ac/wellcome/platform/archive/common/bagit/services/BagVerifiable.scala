package uk.ac.wellcome.platform.archive.common.bagit.services

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.bagit.MatchedLocation
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

    val bagFiles = bag.tagManifest.files ++ bag.manifest.files
    val fetchEntries = bag.fetch.toSeq.flatMap { _.files }

    debug(s"bagFiles: $bagFiles")
    debug(s"fetchEntries: $fetchEntries")

    correlateFetchEntryToBagFile(bagFiles, fetchEntries) match {
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

  protected def correlateFetchEntryToBagFile(
    bagFiles: Seq[BagFile],
    fetchEntries: Seq[BagFetchEntry]
  ): Either[Seq[Throwable], Seq[MatchedLocation]] = {

    case class PathInfo(
      bagFiles: Seq[BagFile] = Seq.empty,
      fetchEntries: Seq[BagFetchEntry] = Seq.empty
    )

    var paths: Map[BagPath, PathInfo] = Map.empty.withDefault { _ =>
      PathInfo()
    }

    bagFiles.foreach { file =>
      val existing = paths(file.path)
      paths = paths ++ Map(
        file.path -> existing.copy(bagFiles = existing.bagFiles :+ file))
    }

    fetchEntries.foreach { fetchEntry =>
      val existing = paths(fetchEntry.path)
      paths = paths ++ Map(
        fetchEntry.path -> existing.copy(
          fetchEntries = existing.fetchEntries :+ fetchEntry))
    }

    val matchedLocations = paths.values.map { pathInfo =>
      (pathInfo.bagFiles.distinct, pathInfo.fetchEntries.distinct) match {
        case (Seq(bagFile), Seq()) =>
          Right(MatchedLocation(bagFile = bagFile, fetchEntry = None))
        case (Seq(bagFile), Seq(fetchEntry)) =>
          Right(
            MatchedLocation(bagFile = bagFile, fetchEntry = Some(fetchEntry)))
        case (Seq(), Seq(fetchEntry)) =>
          Left(
            s"Fetch entry refers to a path that isn't in the bag: $fetchEntry")
        case (Seq(), fetchEntriesForPath) =>
          Left(
            s"Multiple fetch entries refers to a path that isn't in the bag: $fetchEntriesForPath")
        case _ =>
          Left(s"Multiple, ambiguous entries for the same path: $pathInfo")
      }
    }

    val successes = matchedLocations.collect { case Right(t) => t }.toSeq

    val failures = matchedLocations.collect {
      case Left(err) => new Throwable(err)
    }.toSeq

    Either.cond(failures.isEmpty, successes, failures)
  }

  private def getVerifiableLocation(
    matched: MatchedLocation): Either[Throwable, VerifiableLocation] =
    matched match {
      case MatchedLocation(bagFile: BagFile, Some(fetchEntry)) =>
        Right(VerifiableLocation(
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
