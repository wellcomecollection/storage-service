package uk.ac.wellcome.platform.archive.common.bagit.services

import org.scalatest.{Assertion, EitherValues}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.models.BagInfo
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagBuilder,
  StorageRandomThings
}
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.{Location, Prefix}
import uk.ac.wellcome.storage.store.TypedStore

trait BagReaderTestCases[
  Context,
  Namespace,
  BagLocation <: Location,
  BagPrefix <: Prefix[BagLocation]
] extends AnyFunSpec
    with Matchers
    with EitherValues
    with StorageRandomThings
    with BagBuilder[BagLocation, BagPrefix, Namespace]
    with S3Fixtures {

  def withContext[R](testWith: TestWith[Context, R]): R
  def withTypedStore[R](
    testWith: TestWith[TypedStore[BagLocation, String], R]
  )(implicit context: Context): R

  def withBagReader[R](
    testWith: TestWith[BagReader[BagLocation, BagPrefix], R]
  )(
    implicit context: Context
  ): R

  def withNamespace[R](testWith: TestWith[Namespace, R]): R

  def deleteFile(root: BagPrefix, path: String)(
    implicit context: Context
  )

  def scrambleFile(root: BagPrefix, path: String)(
    implicit typedStore: TypedStore[BagLocation, String]
  ): Assertion =
    typedStore.put(root.asLocation(path))(randomAlphanumeric) shouldBe a[
      Right[_, _]
    ]

  def withFixtures[R](
    testWith: TestWith[
      (Context, TypedStore[BagLocation, String], Namespace, Bucket),
      R
    ]
  ): R =
    withContext { implicit context =>
      withTypedStore { typedStore =>
        withNamespace { namespace =>
          withLocalS3Bucket { bucket =>
            testWith((context, typedStore, namespace, bucket))
          }
        }
      }
    }

  it("gets a correctly formed bag") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace, bucket) = fixtures

      val (bagRoot, bagInfo) = createBag()(namespace, bucket)

      val bag = withBagReader {
        _.get(bagRoot).right.value
      }

      bag.info shouldBe bagInfo
    }
  }

  it("errors if the bag-info.txt file does not exist") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace, bucket) = fixtures

      val (bagRoot, _) = createBag()(namespace, bucket)
      deleteFile(bagRoot, path = "bag-info.txt")

      withBagReader {
        _.get(bagRoot).left.value.msg should startWith(
          "Error loading bag-info.txt"
        )
      }
    }
  }

  it("errors if the bag-info.txt file is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace, bucket) = fixtures

      val (bagRoot, _) = createBag()(namespace, bucket)
      scrambleFile(bagRoot, path = "bag-info.txt")

      withBagReader {
        _.get(bagRoot).left.value.msg should startWith(
          "Error loading bag-info.txt"
        )
      }
    }
  }

  it("errors if the file manifest does not exist") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace, bucket) = fixtures

      val (bagRoot, _) = createBag()(namespace, bucket)
      deleteFile(bagRoot, path = "manifest-sha256.txt")

      withBagReader {
        _.get(bagRoot).left.value.msg should startWith(
          "Error loading manifest-sha256.txt"
        )
      }
    }
  }

  it("errors if the file manifest is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace, bucket) = fixtures

      val (bagRoot, _) = createBag()(namespace, bucket)
      scrambleFile(bagRoot, path = "manifest-sha256.txt")

      withBagReader {
        _.get(bagRoot).left.value.msg should startWith(
          "Error loading manifest-sha256.txt"
        )
      }
    }
  }

  it("errors if the tag manifest does not exist") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace, bucket) = fixtures

      val (bagRoot, _) = createBag()(namespace, bucket)
      deleteFile(bagRoot, "tagmanifest-sha256.txt")

      withBagReader {
        _.get(bagRoot).left.value.msg should startWith(
          "Error loading tagmanifest-sha256.txt"
        )
      }
    }
  }

  it("errors if the tag manifest is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace, bucket) = fixtures

      val (bagRoot, _) = createBag()(namespace, bucket)
      scrambleFile(bagRoot, path = "tagmanifest-sha256.txt")

      withBagReader {
        _.get(bagRoot).left.value.msg should startWith(
          "Error loading tagmanifest-sha256.txt"
        )
      }
    }
  }

  it("passes if the fetch.txt does not exist") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace, bucket) = fixtures

      val (bagRoot, _) = createBag()(namespace, bucket)
      deleteFile(bagRoot, path = "fetch.txt")

      withBagReader {
        _.get(bagRoot).right.value.fetch shouldBe None
      }
    }
  }

  it("errors if the fetch file is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace, bucket) = fixtures

      val (bagRoot, _) = createBag()(namespace, bucket)
      scrambleFile(bagRoot, path = "fetch.txt")

      withBagReader {
        _.get(bagRoot).left.value.msg should startWith(
          "Error loading fetch.txt"
        )
      }
    }
  }

  protected def toString(ns: Namespace): String = ns.toString

  protected def createBag()(namespace: Namespace, bucket: Bucket)(
    implicit typedStore: TypedStore[BagLocation, String]
  ): (BagPrefix, BagInfo) = {
    val bagContents = createBagContentsWith()(namespace, bucket)

    storeBagContents(bagContents)

    (bagContents.bagRoot, bagContents.bagInfo)
  }
}
