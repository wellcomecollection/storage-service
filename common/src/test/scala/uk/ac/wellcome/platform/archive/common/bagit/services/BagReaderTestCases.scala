package uk.ac.wellcome.platform.archive.common.bagit.services

import org.scalatest.{Assertion, EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.models.BagInfo
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagBuilder,
  StorageRandomThings
}
import uk.ac.wellcome.storage.ObjectLocation
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

  def deleteFile(rootLocation: ObjectLocation, path: String)(
    implicit context: Context
  )

  def scrambleFile(rootLocation: ObjectLocation, path: String)(
    implicit typedStore: TypedStore[ObjectLocation, String]
  ): Assertion =
    typedStore.put(rootLocation.join(path))(
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

      val (rootLocation, bagInfo) = createBag()

      val bag = withBagReader {
        _.get(rootLocation).right.value
      }

      bag.info shouldBe bagInfo
    }
  }

  it("errors if the bag-info.txt file does not exist") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (rootLocation, _) = createBag()
      deleteFile(rootLocation, "bag-info.txt")

      withBagReader {
        _.get(rootLocation).left.value.msg should startWith(
          "Error loading bag-info.txt"
        )
      }
    }
  }

  it("errors if the bag-info.txt file is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (rootLocation, _) = createBag()
      scrambleFile(rootLocation, "bag-info.txt")

      withBagReader {
        _.get(rootLocation).left.value.msg should startWith(
          "Error loading bag-info.txt"
        )
      }
    }
  }

  it("errors if the file manifest does not exist") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (rootLocation, _) = createBag()
      deleteFile(rootLocation, "manifest-sha256.txt")

      withBagReader {
        _.get(rootLocation).left.value.msg should startWith(
          "Error loading manifest-sha256.txt"
        )
      }
    }
  }

  it("errors if the file manifest is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (rootLocation, _) = createBag()
      scrambleFile(rootLocation, "manifest-sha256.txt")

      withBagReader {
        _.get(rootLocation).left.value.msg should startWith(
          "Error loading manifest-sha256.txt"
        )
      }
    }
  }

  it("errors if the tag manifest does not exist") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (rootLocation, _) = createBag()
      deleteFile(rootLocation, "tagmanifest-sha256.txt")

      withBagReader {
        _.get(rootLocation).left.value.msg should startWith(
          "Error loading tagmanifest-sha256.txt"
        )
      }
    }
  }

  it("errors if the tag manifest is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (rootLocation, _) = createBag()
      scrambleFile(rootLocation, "tagmanifest-sha256.txt")

      withBagReader {
        _.get(rootLocation).left.value.msg should startWith(
          "Error loading tagmanifest-sha256.txt"
        )
      }
    }
  }

  it("passes if the fetch.txt does not exist") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (rootLocation, _) = createBag()
      deleteFile(rootLocation, "fetch.txt")

      withBagReader {
        _.get(rootLocation).right.value.fetch shouldBe None
      }
    }
  }

  it("errors if the fetch file is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures

      val (rootLocation, _) = createBag()
      scrambleFile(rootLocation, "fetch.txt")

      withBagReader {
        _.get(rootLocation).left.value.msg should startWith(
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
  ): (ObjectLocation, BagInfo) = {
    implicit val namespace: String = toString(ns)

    val (bagObjects, bagRoot, bagInfo) = BagBuilder.createBagContentsWith()

    BagBuilder.uploadBagObjects(bagObjects)

    (bagRoot, bagInfo)
  }
}
