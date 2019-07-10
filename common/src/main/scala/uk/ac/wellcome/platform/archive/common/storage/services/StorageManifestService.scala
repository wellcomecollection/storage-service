package uk.ac.wellcome.platform.archive.common.storage.services

import java.time.Instant

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.bagit.models.{Bag, BagManifest, BagPath}
import uk.ac.wellcome.platform.archive.common.bagit.services.BagMatcher
import uk.ac.wellcome.platform.archive.common.ingests.models.{InfrequentAccessStorageProvider, StorageLocation}
import uk.ac.wellcome.platform.archive.common.storage.models.{FileManifest, StorageManifest, StorageManifestFile, StorageSpace}
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

import scala.util.{Failure, Success, Try}

class StorageManifestException(message: String) extends RuntimeException(message)

object StorageManifestService extends Logging {
  def createManifest(
    bag: Bag,
    replicaRoot: ObjectLocation,
    space: StorageSpace,
    version: Int
  ): Try[StorageManifest] = {
    for {
      bagRoot <- getBagRoot(replicaRoot, version)

      entries <- createNamePathMap(bag, bagRoot = bagRoot, version = version)

      fileManifestFiles <- createManifestFiles(
        manifest = bag.manifest,
        entries = entries
      )

      tagManifestFiles <- createManifestFiles(
        manifest = bag.tagManifest,
        entries = entries
      )

      storageManifest = StorageManifest(
        space = space,
        info = bag.info,
        version = version,
        manifest = FileManifest(
          checksumAlgorithm = bag.manifest.checksumAlgorithm,
          files = fileManifestFiles
        ),
        tagManifest = FileManifest(
          checksumAlgorithm = bag.tagManifest.checksumAlgorithm,
          files = tagManifestFiles
        ),
        locations = Seq(
          // TODO: Support adding more locations!
          StorageLocation(
            provider = InfrequentAccessStorageProvider,
            location = bagRoot.asLocation()
          )
        ),
        createdDate = Instant.now
      )
    } yield storageManifest
  }

  /** The replicator writes bags inside a bucket to paths of the form
    *
    *     /{storageSpace}/{externalIdentifier}/v{version}
    *
    * All the versions of a bag should follow this convention, so if we
    * strip off the /:version prefix we'll find the root of all bags
    * with this (storage space, external ID) pair.
    *
    * TODO: It would be better if we passed a structured object out of
    * the replicator.
    *
    */
  private def getBagRoot(replicaRoot: ObjectLocation, version: Int): Try[ObjectLocationPrefix] =
    if (replicaRoot.path.endsWith(s"/v$version")) {
      Success(
        replicaRoot.asPrefix.copy(
          path = replicaRoot.path.stripSuffix(s"/v$version")
        )
      )
    } else {
      Failure(new StorageManifestException(s"Malformed bag root: $replicaRoot (expected suffix /v$version)"))
    }

  /** Every entry in the bag manifest will be either a:
    *
    *   - concrete file inside the replicated bag, or
    *   - a file referenced by the fetch file, which should be in a different
    *     versioned directory under the same bag root
    *
    * This function gets a map (bag name) -> (path), relative to the bag root.
    *
    */
  private def createNamePathMap(
    bag: Bag,
    bagRoot: ObjectLocationPrefix,
    version: Int): Try[Map[BagPath, String]] = Try {
    BagMatcher.correlateFetchEntries(bag) match {
      case Right(matchedLocations) =>
        matchedLocations.map { matchedLoc =>
          val path = matchedLoc.fetchEntry match {
            // This is a concrete file inside the replicated bag,
            // so it's inside the versioned replica directory.
            case None             => s"v$version/${matchedLoc.bagFile.path.value}"

            // This is referring to a fetch file somewhere else.
            // We need to check it's in another versioned directory
            // for this bag.
            case Some(fetchEntry) =>
              val fetchLocation = ObjectLocation(
                namespace = fetchEntry.uri.getHost,
                path = fetchEntry.uri.getPath.stripPrefix("/")
              )

              if (fetchLocation.namespace != bagRoot.namespace) {
                throw new StorageManifestException(
                  s"Fetch entry for ${matchedLoc.bagFile.path.value} refers to an object in the wrong namespace: ${fetchLocation.namespace}"
                )
              }

              // TODO: This check could actually look for a /v1, /v2, etc.
              if (!fetchLocation.path.startsWith(bagRoot.path + "/")) {
                throw new StorageManifestException(
                  s"Fetch entry for ${matchedLoc.bagFile.path.value} refers to an object in the wrong path: /${fetchLocation.path}"
                )
              }

              fetchLocation.path.stripPrefix(bagRoot.path + "/")
          }

          (matchedLoc.bagFile.path, path)
        }.toMap

      case Left(err) =>
        throw new StorageManifestException(
          s"Unable to resolve fetch entries: $err"
        )
    }
  }

  private def createManifestFiles(manifest: BagManifest, entries: Map[BagPath, String]) = Try {
    manifest.files.map { bagFile =>
      // This lookup should never file -- the BagMatcher populates the
      // entries from the original manifests in the bag.
      //
      // We wrap it in a Try block just in case, but this should never
      // throw in practice.
      val path = entries(bagFile.path)

      // If this happens it indicates an error in the pipeline -- we only
      // support bags with a single manifest, so we should only ever see
      // a single algorithm.
      if (bagFile.checksum.algorithm != manifest.checksumAlgorithm) {
        throw new StorageManifestException(
          s"Mismatched checksum algorithms in manifest: entry ${bagFile.path} has algorithm ${bagFile.checksum.algorithm}, but manifest uses ${manifest.checksumAlgorithm}"
        )
      }

      StorageManifestFile(
        checksum = bagFile.checksum.value,
        name = bagFile.path.value,
        path = path
      )
    }
  }
}
