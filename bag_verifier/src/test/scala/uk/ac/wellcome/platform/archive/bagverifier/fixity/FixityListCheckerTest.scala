package uk.ac.wellcome.platform.archive.bagverifier.fixity

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.bagverifier.fixity.bag.BagExpectedFixity
import uk.ac.wellcome.platform.archive.bagverifier.fixity.s3.S3FixityChecker
import uk.ac.wellcome.platform.archive.bagverifier.storage.s3.S3Resolvable
import uk.ac.wellcome.platform.archive.common.bagit.models.Bag
import uk.ac.wellcome.platform.archive.common.bagit.services.s3.S3BagReader
import uk.ac.wellcome.platform.archive.common.fixtures.s3.S3BagBuilder
import uk.ac.wellcome.storage.{StoreWriteError, WriteError}
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.tags.Tags
import uk.ac.wellcome.storage.tags.s3.S3Tags

class FixityListCheckerTest extends AnyFunSpec with Matchers with S3Fixtures {
  describe("handles a failure during tagging") {
    it("a tagging error from S3 is raised") {
      implicit val resolvable: S3Resolvable = new S3Resolvable()

      implicit val verifiable: S3FixityChecker = new S3FixityChecker() {
        override val tags: Tags[S3ObjectLocation] = new S3Tags() {
          override def writeTags(
            location: S3ObjectLocation,
            tags: Map[String, String]
          ): Either[WriteError, Map[String, String]] =
            Left(StoreWriteError(new Throwable("BOOM!")))
        }
      }

      val bagBuilder = new S3BagBuilder {}

      withLocalS3Bucket { bucket =>
        val (bagRoot, _) = bagBuilder.createS3BagWith(bucket)

        implicit val bagExpectedFixity
          : BagExpectedFixity[S3ObjectLocation, S3ObjectLocationPrefix] =
          new BagExpectedFixity[S3ObjectLocation, S3ObjectLocationPrefix](
            bagRoot
          )

        val fixityListChecker
          : FixityListChecker[S3ObjectLocation, S3ObjectLocationPrefix, Bag] =
          new FixityListChecker()

        val bag = new S3BagReader().get(bagRoot).right.get

        fixityListChecker.check(bag) shouldBe a[FixityListWithErrors[_]]
      }
    }
  }
}
