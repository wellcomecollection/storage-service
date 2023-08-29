package weco.storage_service.bag_verifier.verify.steps

import java.net.URI

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage_service.bag_verifier.models.BagVerifierError
import weco.storage_service.bagit.models.{BagFetch, BagFetchMetadata, BagPath}
import weco.storage.fixtures.S3Fixtures
import weco.storage.providers.s3.{S3ObjectLocation, S3ObjectLocationPrefix}

class VerifyFetchTest
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with S3Fixtures {

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
