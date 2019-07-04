package uk.ac.wellcome.platform.archive.common.bagit.services

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  Bag,
  BagFetchEntry,
  BagFile,
  BagPath,
  MatchedLocation
}

/** A bag can contain concrete files or refer to files stored elsewhere
  * in the fetch file.  This object takes a list of files referenced in
  * the manifest and the fetch entries (if any), and works out which
  * are files held outside the main bag.
  *
  */
object BagMatcher {

  def correlateFetchEntries(
    bag: Bag): Either[Seq[Throwable], Seq[MatchedLocation]] =
    correlateFetchEntryToBagFile(
      bagFiles = bag.manifest.files ++ bag.tagManifest.files,
      fetchEntries = bag.fetch match {
        case Some(fetchEntry) => fetchEntry.files
        case None             => Seq.empty
      }
    )

  def correlateFetchEntryToBagFile(
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
}
