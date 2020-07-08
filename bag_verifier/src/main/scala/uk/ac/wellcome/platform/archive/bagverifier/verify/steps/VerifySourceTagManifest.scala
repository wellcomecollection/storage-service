package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import org.apache.commons.io.IOUtils
import uk.ac.wellcome.platform.archive.bagverifier.models.BagVerifierError
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLength
import uk.ac.wellcome.storage.{Identified, Location, Prefix}

trait VerifySourceTagManifest[BagLocation <: Location, BagPrefix <: Prefix[BagLocation]] {
  def streamStore: StreamStore[BagLocation]

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
                                        srcPrefix: BagPrefix,
                                        dstPrefix: BagPrefix
                                      ): Either[BagVerifierError, Unit] = {
    for {
      srcManifest <- getTagManifest(srcPrefix)
      dstManifest <- getTagManifest(dstPrefix)
      result <- if (IOUtils.contentEquals(srcManifest.identifiedT, dstManifest.identifiedT)) {
        Right(())
      } else {
        Left(BagVerifierError(new Throwable(
          "tagmanifest-sha256.txt in replica source and replica location do not match!"
        )))
      }
    } yield result
  }

  private def getTagManifest(prefix: BagPrefix): Either[BagVerifierError, Identified[BagLocation, InputStreamWithLength]] = {
    streamStore.get(
      prefix.asLocation("tagmanifest-sha256.txt")
    ).left.map { error => BagVerifierError(error.e) }
  }
}
