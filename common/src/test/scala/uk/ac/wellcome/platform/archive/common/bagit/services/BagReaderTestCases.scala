package uk.ac.wellcome.platform.archive.common.bagit.services

import org.scalatest.{Assertion, EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.{TypedStore, TypedStoreEntry}

trait BagReaderTestCases[Context, Namespace]
    extends FunSpec
    with Matchers
    with EitherValues
    with BagLocationFixtures[Namespace] {
  def withContext[R](testWith: TestWith[Context, R]): R
  def withTypedStore[R](
    testWith: TestWith[TypedStore[ObjectLocation, String], R])(
    implicit context: Context): R

  def withBagReader[R](testWith: TestWith[BagReader[_], R])(
    implicit context: Context): R

  def withNamespace[R](testWith: TestWith[Namespace, R]): R

  def deleteFile(rootLocation: ObjectLocation, path: String)(
    implicit context: Context)

  def scrambleFile(rootLocation: ObjectLocation, path: String)(
    implicit typedStore: TypedStore[ObjectLocation, String]): Assertion =
    typedStore.put(rootLocation.join(path))(TypedStoreEntry(
      randomAlphanumeric,
      metadata = Map.empty)) shouldBe a[Right[_, _]]

  def withFixtures[R](
    testWith: TestWith[
      (Context, TypedStore[ObjectLocation, String], Namespace),
      R]): R =
    withContext { implicit context =>
      withTypedStore { typedStore =>
        withNamespace { namespace =>
          testWith((context, typedStore, namespace))
        }
      }
    }

  it("gets a correctly formed bag") {
    val bagInfo = createBagInfo

    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures
      withBagReader { bagReader =>
        withBag(bagInfo = bagInfo) { rootLocation =>
          bagReader.get(rootLocation).right.value.info shouldBe bagInfo
        }
      }
    }
  }

  it("errors if the bag-info.txt file does not exist") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures
      withBagReader { bagReader =>
        withBag() { rootLocation =>
          deleteFile(rootLocation, "bag-info.txt")

          bagReader.get(rootLocation).left.value.msg should startWith(
            "Error loading bag-info.txt")
        }
      }
    }
  }

  it("errors if the bag-info.txt file is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures
      withBagReader { bagReader =>
        withBag() { rootLocation =>
          scrambleFile(rootLocation, "bag-info.txt")

          bagReader.get(rootLocation).left.value.msg should startWith(
            "Error loading bag-info.txt")
        }
      }
    }
  }

  it("errors if the file manifest does not exist") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures
      withBagReader { bagReader =>
        withBag() { rootLocation =>
          deleteFile(rootLocation, "manifest-sha256.txt")

          bagReader.get(rootLocation).left.value.msg should startWith(
            "Error loading manifest-sha256.txt")
        }
      }
    }
  }

  it("errors if the file manifest is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures
      withBagReader { bagReader =>
        withBag() { rootLocation =>
          scrambleFile(rootLocation, "manifest-sha256.txt")

          bagReader.get(rootLocation).left.value.msg should startWith(
            "Error loading manifest-sha256.txt")
        }
      }
    }
  }

  it("errors if the tag manifest does not exist") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures
      withBagReader { bagReader =>
        withBag() { rootLocation =>
          deleteFile(rootLocation, "tagmanifest-sha256.txt")

          bagReader.get(rootLocation).left.value.msg should startWith(
            "Error loading tagmanifest-sha256.txt")
        }
      }
    }
  }

  it("errors if the tag manifest is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures
      withBagReader { bagReader =>
        withBag() { rootLocation =>
          scrambleFile(rootLocation, "tagmanifest-sha256.txt")

          bagReader.get(rootLocation).left.value.msg should startWith(
            "Error loading tagmanifest-sha256.txt")
        }
      }
    }
  }

  it("passes if the fetch.txt does not exist") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures
      withBagReader { bagReader =>
        withBag() { rootLocation =>
          deleteFile(rootLocation, "fetch.txt")

          bagReader.get(rootLocation).right.value.fetch shouldBe None
        }
      }
    }
  }

  it("errors if the fetch file is malformed") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace) = fixtures
      withBagReader { bagReader =>
        withBag() { rootLocation =>
          scrambleFile(rootLocation, "fetch.txt")

          bagReader.get(rootLocation).left.value.msg should startWith(
            "Error loading fetch.txt")
        }
      }
    }
  }
}
