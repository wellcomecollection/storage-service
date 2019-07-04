package uk.ac.wellcome.platform.archive.common.bagit.services

import org.scalatest.{Assertion, EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.fixtures.{BagLocationFixtures, S3BagLocationFixtures}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.{TypedStore, TypedStoreEntry}

trait BagReaderTestCases[Namespace] extends FunSpec with Matchers with S3BagLocationFixtures with EitherValues with BagLocationFixtures[Namespace] {
  def withTypedStore[R](testWith: TestWith[TypedStore[ObjectLocation, String], R]): R

  def withNamespace[R](testWith: TestWith[Namespace, R]): R

  def deleteFile(rootLocation: ObjectLocation, path: String)

  val bagReader: BagReader[_]

  def scrambleFile(rootLocation: ObjectLocation, path: String)(implicit typedStore: TypedStore[ObjectLocation, String]): Assertion =
    typedStore.put(rootLocation.join(path))(
      TypedStoreEntry(randomAlphanumeric, metadata = Map.empty)) shouldBe a[Right[_, _]]

  it("gets a correctly formed bag") {
    val bagInfo = createBagInfo

    withTypedStore { implicit typedStore =>
      withNamespace { implicit namespace =>
        withBag(bagInfo = bagInfo) { case (rootLocation, _) =>
          bagReader.get(rootLocation).right.value.info shouldBe bagInfo
        }
      }
    }
  }

  it("errors if the bag-info.txt file does not exist") {
    withTypedStore { implicit typedStore =>
      withNamespace { implicit namespace =>
        withBag() { case (rootLocation, _) =>
          deleteFile(rootLocation, "bag-info.txt")

          bagReader.get(rootLocation).left.value.msg should startWith("Error loading bag-info.txt")
        }
      }
    }
  }

  it("errors if the bag-info.txt file is malformed") {
    withTypedStore { implicit typedStore =>
      withNamespace { implicit namespace =>
        withBag() { case (rootLocation, _) =>
          scrambleFile(rootLocation, "bag-info.txt")

          bagReader.get(rootLocation).left.value.msg should startWith("Error loading bag-info.txt")
        }
      }
    }
  }

  it("errors if the file manifest does not exist") {
    withTypedStore { implicit typedStore =>
      withNamespace { implicit namespace =>
        withBag() { case (rootLocation, _) =>
          deleteFile(rootLocation, "manifest-sha256.txt")

          bagReader.get(rootLocation).left.value.msg should startWith("Error loading manifest-sha256.txt")
        }
      }
    }
  }

  it("errors if the file manifest is malformed") {
    withTypedStore { implicit typedStore =>
      withNamespace { implicit namespace =>
        withBag() { case (rootLocation, _) =>
          scrambleFile(rootLocation, "manifest-sha256.txt")

          bagReader.get(rootLocation).left.value.msg should startWith("Error loading manifest-sha256.txt")
        }
      }
    }
  }

  it("errors if the tag manifest does not exist") {
    withTypedStore { implicit typedStore =>
      withNamespace { implicit namespace =>
        withBag() { case (rootLocation, _) =>
          deleteFile(rootLocation, "tagmanifest-sha256.txt")

          bagReader.get(rootLocation).left.value.msg should startWith("Error loading tagmanifest-sha256.txt")
        }
      }
    }
  }

  it("errors if the tag manifest is malformed") {
    withTypedStore { implicit typedStore =>
      withNamespace { implicit namespace =>
        withBag() { case (rootLocation, _) =>
          scrambleFile(rootLocation, "tagmanifest-sha256.txt")

          bagReader.get(rootLocation).left.value.msg should startWith("Error loading tagmanifest-sha256.txt")
        }
      }
    }
  }

  it("passes if the fetch.txt does not exist") {
    withTypedStore { implicit typedStore =>
      withNamespace { implicit namespace =>
        withBag() { case (rootLocation, _) =>
          deleteFile(rootLocation, "fetch.txt")

          bagReader.get(rootLocation).right.value.fetch shouldBe None
        }
      }
    }
  }

  it("errors if the fetch file is malformed") {
    withTypedStore { implicit typedStore =>
      withNamespace { implicit namespace =>
        withBag() { case (rootLocation, _) =>
          scrambleFile(rootLocation, "fetch.txt")

          bagReader.get(rootLocation).left.value.msg should startWith("Error loading fetch.txt")
        }
      }
    }
  }
}
