package uk.ac.wellcome.platform.archive.bagverifier.services.s3

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.services.BagVerifier
import uk.ac.wellcome.storage.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.store.TypedStore

class S3ReplicatedBagVerifierTest extends S3BagVerifierTestCases {
  override def withVerifier[R](primaryBucket: Bucket)(
    testWith: TestWith[BagVerifier[S3ObjectLocation, S3ObjectLocationPrefix], R]
  )(
    implicit typedStore: TypedStore[S3ObjectLocation, String]
  ): R =
    testWith(
      new S3ReplicatedBagVerifier(primaryBucket = primaryBucket.name)
    )
}
