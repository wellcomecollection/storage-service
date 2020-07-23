package uk.ac.wellcome.platform.archive.bag_register.services

import java.net.URI
import java.time.Instant

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.bagit.models._
import uk.ac.wellcome.platform.archive.common.bagit.services.BagMatcher
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.platform.archive.common.storage.models._
import uk.ac.wellcome.platform.archive.common.storage.services.SizeFinder
import uk.ac.wellcome.platform.archive.common.storage.services.s3.S3SizeFinder
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.Readable
import uk.ac.wellcome.storage.store.s3.NewS3StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

import scala.util.{Failure, Success, Try}

class S3StorageManifestService(implicit s3Client: AmazonS3) extends Logging {
  val sizeFinder: SizeFinder[S3ObjectLocation] =
    new S3SizeFinder()

  implicit val streamReader: Readable[S3ObjectLocation, InputStreamWithLength] =
    new NewS3StreamStore()

  private lazy val tagManifestFileFinder = new TagManifestFileFinder()

  def createLocation(uri: URI): S3ObjectLocation =
    new S3ObjectLocation(
      bucket = uri.getHost,
      key = uri.getPath.stripPrefix("/")
    )

  def createManifest(
    ingestId: IngestID,
    bag: Bag,
    location: PrimaryStorageLocation,
    replicas: Seq[SecondaryStorageLocation],
    space: StorageSpace,
    version: BagVersion
  ): Try[StorageManifest] =
    for {
      root: ObjectLocationPrefix <- getBagRoot(location.prefix, version)

      bagRoot = S3ObjectLocationPrefix(root)

      replicaLocations <- getReplicaLocations(replicas, version)

      matchedLocations <- resolveFetchLocations(bag)

      entries <- createPathLocationMap(
        matchedLocations = matchedLocations,
        bagRoot = bagRoot,
        version = version
      )

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

      unreferencedTagManifestFiles <- getUnreferencedFiles(
        bagRoot = bagRoot,
        version = version,
        tagManifest = bag.tagManifest
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
          files = tagManifestFiles ++ unreferencedTagManifestFiles
        ),
        location = PrimaryStorageLocation(
          provider = location.provider,
          prefix = bagRoot.toObjectLocationPrefix
        ),
        replicaLocations = replicaLocations,
        createdDate = Instant.now,
        ingestId = ingestId
      )
    } yield storageManifest

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
  private def getBagRoot(
    replicaRoot: ObjectLocationPrefix,
    version: BagVersion
  ): Try[ObjectLocationPrefix] =
    if (replicaRoot.path.endsWith(s"/$version")) {
      Success(
        replicaRoot.copy(
          path = replicaRoot.path.stripSuffix(s"/$version")
        )
      )
    } else {
      Failure(
        new StorageManifestException(
          s"Malformed bag root: $replicaRoot (expected suffix /$version)"
        )
      )
    }

  private def resolveFetchLocations(bag: Bag): Try[Seq[MatchedLocation]] =
    BagMatcher.correlateFetchEntries(bag) match {
      case Right(matchedLocations) => Success(matchedLocations)
      case Left(err) =>
        Failure(
          new StorageManifestException(
            s"Unable to resolve fetch entries: $err"
          )
        )
    }

  private def getSizeAndLocation(
    matchedLocation: MatchedLocation,
    bagRoot: S3ObjectLocationPrefix,
    version: BagVersion
  ): (S3ObjectLocation, Option[Long]) =
    matchedLocation.fetchMetadata match {
      // A concrete file inside the replicated bag, so it's inside
      // the versioned replica directory.
      case None =>
        (
          bagRoot.asLocation(version.toString, matchedLocation.bagPath.value),
          None
        )

      // An entry in the fetch.txt, referring to a file somewhere else.
      case Some(fetchMetadata) =>
        (
          createLocation(fetchMetadata.uri),
          fetchMetadata.length
        )
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
    matchedLocations: Seq[MatchedLocation],
    bagRoot: S3ObjectLocationPrefix,
    version: BagVersion
  ): Try[Map[BagPath, (S3ObjectLocation, Option[Long])]] =
    Try {
      matchedLocations.map { matchedLoc =>
        (
          matchedLoc.bagPath,
          getSizeAndLocation(matchedLoc, bagRoot, version)
        )
      }.toMap
    }

  private def createManifestFiles(
    manifest: BagManifest,
    entries: Map[BagPath, (S3ObjectLocation, Option[Long])],
    bagRoot: S3ObjectLocationPrefix
  ): Try[Seq[StorageManifestFile]] = Try {
    manifest.entries.map {
      case (bagPath, checksumValue) =>
        // This lookup should never file -- the BagMatcher populates the
        // entries from the original manifests in the bag.
        //
        // The bag verifier should already have checked that any entries in
        // the fetch.txt are inside the bag in the storage service.
        //
        // We wrap it in a Try block just in case, but this should never
        // throw in practice.
        val (location, maybeSize) = entries(bagPath)

        assert(
          location.path.startsWith(bagRoot.pathPrefix + "/"),
          s"Looks like a fetch.txt URI wasn't under the bag root - why wasn't this spotted by the verifier? +" +
            s"$location ($bagPath)"
        )
        val path = location.path.stripPrefix(bagRoot.pathPrefix + "/")

        val size = maybeSize match {
          case Some(s) => s
          case None =>
            sizeFinder.getSize(location) match {
              case Right(value) => value
              case Left(readError) =>
                throw new StorageManifestException(
                  s"Error getting size of $location: ${readError.e}"
                )
            }
        }

        StorageManifestFile(
          checksum = checksumValue,
          name = bagPath.value,
          path = path,
          size = size
        )
    }.toSeq
  }

  // Get StorageManifestFile entries for everything not listed in either the manifest
  // or tag manifest BagIt files.
  //
  // This should only be the tagmanifest-*.txt files -- the verifier checks that these
  // are the only unreferenced files.
  //
  private def getUnreferencedFiles(
    bagRoot: S3ObjectLocationPrefix,
    version: BagVersion,
    tagManifest: BagManifest
  ): Try[Seq[StorageManifestFile]] =
    tagManifestFileFinder
      .getTagManifestFiles(
        prefix = bagRoot.join(version.toString),
        algorithm = tagManifest.checksumAlgorithm
      )
      .map {
        // Remember to prefix all the entries with a version string
        _.map { f =>
          f.copy(path = s"$version/${f.path}")
        }
      }

  private def getReplicaLocations(
    replicas: Seq[SecondaryStorageLocation],
    version: BagVersion
  ): Try[Seq[SecondaryStorageLocation]] = {
    val rootReplicas =
      replicas
        .map { loc =>
          getBagRoot(replicaRoot = loc.prefix, version = version) match {
            case Success(prefix) =>
              Success(
                SecondaryStorageLocation(
                  provider = loc.provider,
                  prefix = prefix
                )
              )

            case Failure(err) => Failure(err)
          }
        }

    val successes = rootReplicas.collect { case Success(loc) => loc }
    val failures = rootReplicas.collect { case Failure(err)  => err }

    if (failures.isEmpty) {
      Success(successes)
    } else {
      Failure(
        new StorageManifestException(
          s"Malformed bag root in the replicas: $failures"
        )
      )
    }
  }
}
