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
import uk.ac.wellcome.storage.{Location, Prefix}
import uk.ac.wellcome.storage.store.TypedStore

trait BagReaderTestCases[
  Context,
  Namespace,
  BagLocation <: Location,
  BagLocationPrefix <: Prefix[BagLocation]
] extends AnyFunSpec
    with Matchers
    with EitherValues
    with StorageRandomThings
    with BagBuilder[BagLocation, BagLocationPrefix] {
  def withContext[R](testWith: TestWith[Context, R]): R
  def withTypedStore[R](
    testWith: TestWith[TypedStore[BagLocation, String], R]
  )(implicit context: Context): R

  def withBagReader[R](testWith: TestWith[BagReader, R])(
    implicit context: Context
  ): R

  def withNamespace[R](testWith: TestWith[Namespace, R]): R

  def deleteFile(root: BagLocationPrefix, path: String)(
    implicit context: Context
  )

  def scrambleFile(root: BagLocationPrefix, path: String)(
    implicit typedStore: TypedStore[BagLocation, String]
  ): Assertion =
    typedStore.put(root.asLocation(path))(randomAlphanumeric) shouldBe a[
      Right[_, _]
    ]

  def withFixtures[R](
    testWith: TestWith[
      (Context, TypedStore[BagLocation, String], Namespace),
      R
    ]
  ): R =
    withContext { implicit context =>
      withTypedStore { typedStore =>
        withNamespace { namespace =>
          testWith((context, typedStore, namespace))
        }
      }
    }

  it("gets a correctly formed bag") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (bagRoot, bagInfo) = createBag()

      val bag = withBagReader {
        _.get(bagRoot.toObjectLocationPrefix).right.value
      }

      bag.info shouldBe bagInfo
    }
  }

  it("errors if the bag-info.txt file does not exist") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (bagRoot, _) = createBag()
      deleteFile(bagRoot, path = "bag-info.txt")

      withBagReader {
        _.get(bagRoot.toObjectLocationPrefix).left.value.msg should startWith(
          "Error loading bag-info.txt"
        )
      }
    }
  }

  it("errors if the bag-info.txt file is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (bagRoot, _) = createBag()
      scrambleFile(bagRoot, path = "bag-info.txt")

      withBagReader {
        _.get(bagRoot.toObjectLocationPrefix).left.value.msg should startWith(
          "Error loading bag-info.txt"
        )
      }
    }
  }

  it("errors if the file manifest does not exist") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (bagRoot, _) = createBag()
      deleteFile(bagRoot, path = "manifest-sha256.txt")

      withBagReader {
        _.get(bagRoot.toObjectLocationPrefix).left.value.msg should startWith(
          "Error loading manifest-sha256.txt"
        )
      }
    }
  }

  it("errors if the file manifest is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (bagRoot, _) = createBag()
      scrambleFile(bagRoot, path = "manifest-sha256.txt")

      withBagReader {
        _.get(bagRoot.toObjectLocationPrefix).left.value.msg should startWith(
          "Error loading manifest-sha256.txt"
        )
      }
    }
  }

  it("errors if the tag manifest does not exist") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (bagRoot, _) = createBag()
      deleteFile(bagRoot, "tagmanifest-sha256.txt")

      withBagReader {
        _.get(bagRoot.toObjectLocationPrefix).left.value.msg should startWith(
          "Error loading tagmanifest-sha256.txt"
        )
      }
    }
  }

  it("errors if the tag manifest is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (bagRoot, _) = createBag()
      scrambleFile(bagRoot, path = "tagmanifest-sha256.txt")

      withBagReader {
        _.get(bagRoot.toObjectLocationPrefix).left.value.msg should startWith(
          "Error loading tagmanifest-sha256.txt"
        )
      }
    }
  }

  it("passes if the fetch.txt does not exist") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (bagRoot, _) = createBag()
      deleteFile(bagRoot, path = "fetch.txt")

      withBagReader {
        _.get(bagRoot.toObjectLocationPrefix).right.value.fetch shouldBe None
      }
    }
  }

  it("errors if the fetch file is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (bagRoot, _) = createBag()
      scrambleFile(bagRoot, path = "fetch.txt")

      withBagReader {
        _.get(bagRoot.toObjectLocationPrefix).left.value.msg should startWith(
          "Error loading fetch.txt"
        )
      }
    }
  }

  protected def toString(ns: Namespace): String = ns.toString

  protected def createBag()(
    implicit
    ns: Namespace,
    typedStore: TypedStore[BagLocation, String]
  ): (BagLocationPrefix, BagInfo) = {
    implicit val namespace: String = toString(ns)

    val (bagObjects, bagRoot, bagInfo) = createBagContentsWith()

    uploadBagObjects(bagObjects)

    (bagRoot, bagInfo)
  }
}
