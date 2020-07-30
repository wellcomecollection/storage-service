package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import org.apache.commons.io.IOUtils
import uk.ac.wellcome.platform.archive.bagverifier.models.BagVerifierError
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLength
import uk.ac.wellcome.storage.{Identified, Location, Prefix}

trait VerifySourceTagManifest[SrcBagLocation <: Location, ReplicaBagLocation <: Location] {
  protected val srcStreamStore: StreamStore[SrcBagLocation]
  protected val replicaStreamStore: StreamStore[ReplicaBagLocation]

  /** This step is here to check the bag created by the replica and the
    * original bag are the same; the verifier can only check that a
    * bag is correctly formed.
    *
    * Without this check, it would be possible for the replicator to
    * write an entirely different, valid bag -- and because the verifier
    * doesn't have context for the original bag, it wouldn't flag
    * it as an error.
    *
    */
  def verifySourceTagManifestIsTheSame(
    srcPrefix: Prefix[SrcBagLocation],
    replicaPrefix: Prefix[ReplicaBagLocation]
  ): Either[BagVerifierError, Unit] = {
    for {
      srcManifest <- getTagManifest(srcPrefix, srcStreamStore)
      replicaManifest <- getTagManifest(replicaPrefix, replicaStreamStore)

      result <- if (IOUtils.contentEquals(srcManifest, replicaManifest)) {
        Right(())
      } else {
        Left(
          BagVerifierError(
            new Throwable(
              "tagmanifest-sha256.txt in replica source and replica location do not match!"
            )
          )
        )
      }
    } yield result
  }

  private def getTagManifest[BagLocation <: Location](
    prefix: Prefix[BagLocation], streamStore: StreamStore[BagLocation]
  ): Either[BagVerifierError, InputStreamWithLength] =
    streamStore.get(prefix.asLocation("tagmanifest-sha256.txt")) match {
      case Right(Identified(_, inputStream)) => Right(inputStream)
      case Left(error)                       => Left(BagVerifierError(error.e))
    }
}
