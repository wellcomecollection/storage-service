package uk.ac.wellcome.platform.archive.bagverifier.verify.steps

import java.net.URI

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.bagverifier.models.BagVerifierError
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagFetch,
  BagFetchMetadata,
  BagPath
}
import uk.ac.wellcome.storage.fixtures.NewS3Fixtures
import uk.ac.wellcome.storage.{S3ObjectLocation, S3ObjectLocationPrefix}

class VerifyFetchTest
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with NewS3Fixtures {

  val verifier: VerifyFetch[S3ObjectLocation, S3ObjectLocationPrefix] =
    new VerifyFetch[S3ObjectLocation, S3ObjectLocationPrefix] {}

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
          root =
            S3ObjectLocationPrefix(bucket = "my-bukkit", keyPrefix = "b1234")
        )

      // First check this is a valid fetch.txt if the URI points to an S3 bucket
      getVerifyResult(scheme = "s3") shouldBe Right(())

      // Then check that it's *not* a valid fetch.txt if the URI points elsewhere
      getVerifyResult(scheme = "https").left.value.userMessage.get should startWith(
        "fetch.txt refers to paths in a mismatched prefix or with a non-S3 URI scheme:"
      )
    }
  }

  describe("verifyNoConcreteFetchEntries") {
    it("ignores a bag with no fetch entries") {
      verifier.verifyNoConcreteFetchEntries(
        fetch = None,
        root = createS3ObjectLocationPrefix,
        actualLocations = (1 to 5).map { _ =>
          createS3ObjectLocation
        }
      ) shouldBe Right(())
    }
  }
}
