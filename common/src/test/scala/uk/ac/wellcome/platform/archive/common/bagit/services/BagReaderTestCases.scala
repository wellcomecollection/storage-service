package uk.ac.wellcome.platform.archive.common.bagit.services

import org.scalatest.{Assertion, EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.models.BagInfo
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagBuilder,
  StorageRandomThings
}
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}
import uk.ac.wellcome.storage.store.{TypedStore, TypedStoreEntry}

trait BagReaderTestCases[Context, Namespace]
    extends FunSpec
    with Matchers
    with EitherValues
    with StorageRandomThings {
  def withContext[R](testWith: TestWith[Context, R]): R
  def withTypedStore[R](
    testWith: TestWith[TypedStore[ObjectLocation, String], R]
  )(implicit context: Context): R

  def withBagReader[R](testWith: TestWith[BagReader[_], R])(
    implicit context: Context
  ): R

  def withNamespace[R](testWith: TestWith[Namespace, R]): R

  def deleteFile(bagRoot: ObjectLocationPrefix, path: String)(
    implicit context: Context
  )

  def scrambleFile(bagRoot: ObjectLocationPrefix, path: String)(
    implicit typedStore: TypedStore[ObjectLocation, String]
  ): Assertion =
    typedStore.put(bagRoot.asLocation(path))(
      TypedStoreEntry(randomAlphanumeric, metadata = Map.empty)
    ) shouldBe a[Right[_, _]]

  def withFixtures[R](
    testWith: TestWith[
      (Context, TypedStore[ObjectLocation, String], Namespace),
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
        _.get(bagRoot).right.value
      }

      bag.info shouldBe bagInfo
    }
  }

  it("errors if the bag-info.txt file does not exist") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (bagRoot, _) = createBag()
      deleteFile(bagRoot, "bag-info.txt")

      withBagReader {
        _.get(bagRoot).left.value.msg should startWith(
          "Error loading bag-info.txt"
        )
      }
    }
  }

  it("errors if the bag-info.txt file is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (bagRoot, _) = createBag()
      scrambleFile(bagRoot, "bag-info.txt")

      withBagReader {
        _.get(bagRoot).left.value.msg should startWith(
          "Error loading bag-info.txt"
        )
      }
    }
  }

  it("errors if the file manifest does not exist") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (bagRoot, _) = createBag()
      deleteFile(bagRoot, "manifest-sha256.txt")

      withBagReader {
        _.get(bagRoot).left.value.msg should startWith(
          "Error loading manifest-sha256.txt"
        )
      }
    }
  }

  it("errors if the file manifest is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (bagRoot, _) = createBag()
      scrambleFile(bagRoot, "manifest-sha256.txt")

      withBagReader {
        _.get(bagRoot).left.value.msg should startWith(
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
        _.get(bagRoot).left.value.msg should startWith(
          "Error loading tagmanifest-sha256.txt"
        )
      }
    }
  }

  it("errors if the tag manifest is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (bagRoot, _) = createBag()
      scrambleFile(bagRoot, "tagmanifest-sha256.txt")

      withBagReader {
        _.get(bagRoot).left.value.msg should startWith(
          "Error loading tagmanifest-sha256.txt"
        )
      }
    }
  }

  it("passes if the fetch.txt does not exist") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (bagRoot, _) = createBag()
      deleteFile(bagRoot, "fetch.txt")

      withBagReader {
        _.get(bagRoot).right.value.fetch shouldBe None
      }
    }
  }

  it("errors if the fetch file is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (bagRoot, _) = createBag()
      scrambleFile(bagRoot, "fetch.txt")

      withBagReader {
        _.get(bagRoot).left.value.msg should startWith(
          "Error loading fetch.txt"
        )
      }
    }
  }

  protected def toString(ns: Namespace): String = ns.toString

  protected def createBag()(
    implicit
    ns: Namespace,
    typedStore: TypedStore[ObjectLocation, String]
  ): (ObjectLocationPrefix, BagInfo) = {
    implicit val namespace: String = toString(ns)

    val (bagObjects, bagRoot, bagInfo) = BagBuilder.createBagContentsWith()

    BagBuilder.uploadBagObjects(bagObjects)

    (bagRoot, bagInfo)
  }
}
