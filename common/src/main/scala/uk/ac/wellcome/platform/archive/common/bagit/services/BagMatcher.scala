package uk.ac.wellcome.platform.archive.common.bagit.services

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  Bag,
  BagFetchEntry,
  BagFetchMetadata,
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
    bag: Bag
  ): Either[Throwable, Seq[MatchedLocation]] =
    correlateFetchEntryToBagFile(
      bagFiles = bag.manifest.files ++ bag.tagManifest.files,
      fetchEntries = bag.fetch match {
        case Some(fetchEntry) => fetchEntry.entries
        case None             => Map.empty
      }
    )

  def correlateFetchEntryToBagFile(
    bagFiles: Seq[BagFile],
    fetchEntries: Map[BagPath, BagFetchMetadata]
  ): Either[Throwable, Seq[MatchedLocation]] = {
    // Each path should only appear once in the list of BagPaths; when that list
    // is a Map, that will be enforced by the time system.  Until then, we have to
    // look for duplicates manually.
    // TODO: Remove this line.
    val duplicateBagFiles: Map[BagPath, Seq[BagFile]] = bagFiles
      .groupBy { _.path }
      .filter { case (_, files) => files.distinct.size > 1 }

    // First construct the list of matched locations -- for every file in the bag,
    // we either have a fetch.txt entry or we don't.
    val matchedLocations =
      bagFiles
        .distinct
        .map { bagFile =>
          fetchEntries.get(bagFile.path) match {
            case Some(fetchMetadata) =>
              MatchedLocation(
                bagFile = bagFile,
                fetchEntry = Some(
                  BagFetchEntry(
                    uri = fetchMetadata.uri,
                    length = fetchMetadata.length,
                    path = bagFile.path
                  )
                )
              )

            case None => MatchedLocation(bagFile = bagFile, fetchEntry = None)
          }
        }

    // We also need to check whether there are any fetch entries which don't appear in
    // the list of BagFiles (i.e., the manifest).
    //
    // If they are, we should throw an error.
    val manifestPaths = bagFiles.map { _.path }.toSet
    val fetchPaths = fetchEntries.collect { case (bagPath, _) => bagPath }.toSet

    val unexpectedFetchPaths = fetchPaths.diff(manifestPaths)

    if (unexpectedFetchPaths.nonEmpty) {
      val pathString = unexpectedFetchPaths
        .map { _.value }
        .toList
        .sorted
        .mkString(", ")

      Left(
        new RuntimeException(
          s"fetch.txt refers to paths that aren't in the bag manifest: $pathString"
        )
      )
    } else if (duplicateBagFiles.nonEmpty) {
      val pathString = duplicateBagFiles
        .map { case (bagPath, _) => bagPath.value }
        .toList
        .sorted
        .mkString(", ")

      Left(
        new RuntimeException(s"Multiple, ambiguous entries for the same path: $pathString")
      )
    } else {
      Right(matchedLocations)
    }
  }
}
