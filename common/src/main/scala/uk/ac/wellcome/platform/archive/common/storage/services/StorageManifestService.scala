package uk.ac.wellcome.platform.archive.common.storage.services

import java.time.Instant

import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.bagit.models.{Bag, BagManifest}
import uk.ac.wellcome.platform.archive.common.storage.models.{StorageManifest, StorageSpace}
import uk.ac.wellcome.platform.archive.common.verify.SHA256
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

import scala.util.{Failure, Success, Try}

class StorageManifestException(message: String) extends RuntimeException(message)

object StorageManifestService extends Logging {
  def createManifest(
    bag: Bag,
    replicaRootLocation: ObjectLocation,
    version: Int
  ): Try[StorageManifest] = {
    for {
      bagRoot <- getBagRoot(replicaRootLocation, version)
      _ = debug(s"Bag root is $bagRoot")
      sm = StorageManifest(
        space = StorageSpace("123"),
        info = bag.info,
        version = version,
        manifest = BagManifest(
          checksumAlgorithm = SHA256,
          files = Seq.empty
        ),
        tagManifest = BagManifest(
          checksumAlgorithm = SHA256,
          files = Seq.empty
        ),
        locations = List.empty,
        createdDate = Instant.now
      )
    } yield sm
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
  private def getBagRoot(replicaRootLocation: ObjectLocation, version: Int): Try[ObjectLocationPrefix] =
    if (replicaRootLocation.path.endsWith(s"/v$version")) {
      Success(
        replicaRootLocation.asPrefix.copy(
          path = replicaRootLocation.path.stripSuffix(s"/v$version")
        )
      )
    } else {
      Failure(new StorageManifestException(s"Malformed bag root: $replicaRootLocation (expected suffix /v$version)"))
    }
}
