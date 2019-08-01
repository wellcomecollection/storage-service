package uk.ac.wellcome.platform.archive.common.storage.services

import java.time.Instant

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  Bag,
  BagManifest,
  BagPath,
  BagVersion
}
import uk.ac.wellcome.platform.archive.common.bagit.services.BagMatcher
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  InfrequentAccessStorageProvider,
  StorageLocation
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  FileManifest,
  StorageManifest,
  StorageManifestFile,
  StorageSpace
}
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

import scala.util.{Failure, Success, Try}

class StorageManifestException(message: String)
    extends RuntimeException(message)

class StorageManifestService(sizeFinder: SizeFinder) extends Logging {
  def createManifest(
    bag: Bag,
    replicaRoot: ObjectLocation,
    space: StorageSpace,
    version: BagVersion
  ): Try[StorageManifest] = {
    for {
      bagRoot <- getBagRoot(replicaRoot, version)

      entries <- createPathLocationMap(
        bag,
        bagRoot = bagRoot,
        version = version)

      fileManifestFiles <- createManifestFiles(
        bagRoot = bagRoot,
        manifest = bag.manifest,
        entries = entries
      )

      tagManifestFiles <- createManifestFiles(
        bagRoot = bagRoot,
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
  private def getBagRoot(replicaRoot: ObjectLocation,
                         version: BagVersion): Try[ObjectLocationPrefix] =
    if (replicaRoot.path.endsWith(s"/$version")) {
      Success(
        replicaRoot.asPrefix.copy(
          path = replicaRoot.path.stripSuffix(s"/$version")
        )
      )
    } else {
      Failure(
        new StorageManifestException(
          s"Malformed bag root: $replicaRoot (expected suffix /$version)"))
    }

  /** Every entry in the bag manifest will be either a:
    *
    *   - concrete file inside the replicated bag, or
    *   - a file referenced by the fetch file, which should be in a different
    *     versioned directory under the same bag root
    *
    * The map contains data
    *
    *   (bag path) -> (location, size if known)
    *
    */
  private def createPathLocationMap(
    bag: Bag,
    bagRoot: ObjectLocationPrefix,
    version: BagVersion): Try[Map[BagPath, (ObjectLocation, Option[Long])]] =
    Try {
      BagMatcher.correlateFetchEntries(bag) match {
        case Right(matchedLocations) =>
          matchedLocations.map { matchedLoc =>
            val location = matchedLoc.fetchEntry match {
              // This is a concrete file inside the replicated bag,
              // so it's inside the versioned replica directory.
              case None =>
                (
                  bagRoot.asLocation(
                    version.toString,
                    matchedLoc.bagFile.path.value),
                  None
                )

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

                (fetchLocation, fetchEntry.length)
            }

            (matchedLoc.bagFile.path, location)
          }.toMap

        case Left(err) =>
          throw new StorageManifestException(
            s"Unable to resolve fetch entries: $err"
          )
      }
    }

  private def createManifestFiles(
    manifest: BagManifest,
    entries: Map[BagPath, (ObjectLocation, Option[Long])],
    bagRoot: ObjectLocationPrefix) = Try {
    manifest.files.map { bagFile =>
      // This lookup should never file -- the BagMatcher populates the
      // entries from the original manifests in the bag.
      //
      // We wrap it in a Try block just in case, but this should never
      // throw in practice.
      val (location, maybeSize) = entries(bagFile.path)
      val path = location.path.stripPrefix(bagRoot.path + "/")

      // If this happens it indicates an error in the pipeline -- we only
      // support bags with a single manifest, so we should only ever see
      // a single algorithm.
      if (bagFile.checksum.algorithm != manifest.checksumAlgorithm) {
        throw new StorageManifestException(
          "Mismatched checksum algorithms in manifest: " +
            s"entry ${bagFile.path} has algorithm ${bagFile.checksum.algorithm}, " +
            s"but manifest uses ${manifest.checksumAlgorithm}"
        )
      }

      val size = maybeSize match {
        case Some(s) => s
        case None =>
          sizeFinder.getSize(location) match {
            case Success(value) => value
            case Failure(err) =>
              throw new StorageManifestException(
                s"Error getting size of $location: $err")
          }
      }

      StorageManifestFile(
        checksum = bagFile.checksum.value,
        name = bagFile.path.value,
        path = path,
        size = size
      )
    }
  }
}
