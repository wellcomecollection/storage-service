package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import java.net.URI

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.bagverifier.models.BagVerifierError
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagFetch, BagFetchMetadata, BagPath}
import uk.ac.wellcome.storage.ObjectLocationPrefix
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators

class VerifyFetchTest
  extends AnyFunSpec
    with Matchers
    with EitherValues
    with ObjectLocationGenerators {

  val verifier: VerifyFetch = new VerifyFetch {}

  describe("verifyFetchPrefixes") {
    it("detects a non-S3 URI") {
      def getVerifyResult(scheme: String): Either[BagVerifierError, Unit] =
        verifier.verifyFetchPrefixes(
          fetch = Some(
            BagFetch(
              entries = Map(
                BagPath("data/cat.jpg") -> BagFetchMetadata(
                  uri = new URI(s"$scheme://my-bukkit/b1234/data/cat.jpg"),
                  length = None
                )
              )
            )
          ),
          root = ObjectLocationPrefix(namespace = "my-bukkit", path = "b1234")
        )

      // First check this is a valid fetch.txt if the URI points to an S3 bucket
      getVerifyResult(scheme = "s3") shouldBe Right(())

      // Then check that it's *not* a valid fetch.txt if the URI points elsewhere
      getVerifyResult(scheme = "https")
        .left.value
        .userMessage.get should startWith("fetch.txt refers to paths in a mismatched prefix")
    }
  }
}
