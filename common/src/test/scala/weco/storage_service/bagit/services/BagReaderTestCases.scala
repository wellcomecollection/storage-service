package weco.storage_service.bagit.services

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, EitherValues}
import weco.fixtures.TestWith
import weco.storage.fixtures.S3Fixtures
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.store.TypedStore
import weco.storage.{Location, Prefix}
import weco.storage_service.bagit.models._
import weco.storage_service.fixtures.BagBuilder
import weco.storage_service.generators.StorageRandomGenerators
import weco.storage_service.verify.ChecksumValue

import java.time.LocalDate

trait BagReaderTestCases[
  Context,
  Namespace,
  BagLocation <: Location,
  BagPrefix <: Prefix[BagLocation]
] extends AnyFunSpec
    with Matchers
    with EitherValues
    with StorageRandomGenerators
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
    typedStore.put(root.asLocation(path))(randomAlphanumeric()) shouldBe a[
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
        _.get(bagRoot).value
      }

      bag.info shouldBe bagInfo
    }
  }

  it("gets a bag with multiple manifests") {
    withFixtures { fixtures =>
      implicit val (context, typedStore, namespace, bucket) = fixtures

      val space = createStorageSpace
      val externalIdentifier = ExternalIdentifier("multiple_manifests")
      val version = createBagVersion

      val bagRoot: BagPrefix = createBagRoot(space, externalIdentifier, version)(namespace)

      val bagInfo = BagInfo(
        payloadOxum = PayloadOxum(payloadBytes = 15, numberOfPayloadFiles = 1),
        externalIdentifier = externalIdentifier,
        baggingDate = LocalDate.now()
      )

      val bagContents = BagContents(
        fetchObjects = Map(),
        bagObjects = Map(
          bagRoot.asLocation("data/README.txt") -> "This is a file\n",
          bagRoot.asLocation("bag-info.txt") -> (
            "Bagging-Date: 2021-07-16\n" +
              "External-Identifier: multiple_manifests\n" +
              "Payload-Oxum: 15.1\n"
            ),
          bagRoot.asLocation("bagit.txt") -> (
            "BagIt-Version: 0.97\n" +
              "Tag-File-Character-Encoding: UTF-8\n"
            ),

          bagRoot.asLocation("manifest-sha512.txt") -> "7cd31c95fc5a40e5be7bf46e84df52c6d8d50e9003dfb7e3b85ac9c704b90a63ac220147645ff22d410166356133d241a7346e452c863601ce68b82d075031f8  data/README.txt\n",
          bagRoot.asLocation("manifest-md5.txt") -> "a86e2699931d4f3d1456e79383749e43  data/README.txt\n",

          bagRoot.asLocation("tagmanifest-sha512.txt") -> (
            "b7112a34f6892c1d3bfb6054dc4977c2ffd32bd7e4d8b686d08f68d1ef407c35857ad3cf552543318238701afb390faad20ac7a0a22b1cf43cd916dfb5d97efa  bag-info.txt\n" +
              "418dcfbe17d5f4b454b18630be795462cf7da4ceb6313afa49451aa2568e41f7ca3d34cf0280c7d056dc5681a70c37586aa1755620520b9198eede905ba2d0f6  bagit.txt\n" +
              "bfbd969850673f65d14917bcbe42e86df867e4e383702a4471eb0776f2f1cfa48ec102489416741dcf278344bc0229ac2a9011080ffe2a4e55a64540ed0291d9  manifest-sha512.txt\n" +
              "f8036c779eba074e72101458d675c287b731f5bec4cbe744d59565ce4cc26f96d5259d8f7b1cc55f3999d4db34eba59d99dc131200f1bdf8ddc89912ed23afe6  manifest-md5.txt\n"
          ),
          bagRoot.asLocation("tagmanifest-md5.txt") -> (
            "aa3c5e977224a9186dbb36ef1193be0d  bag-info.txt\n" +
              "9e5ad981e0d29adc278f6a294b8c2aca  bagit.txt\n" +
              "d570da37be627c3955c165422e667245  manifest-sha512.txt\n" +
              "7983626d0844789acfe8059b6730b9d1  manifest-md5.txt\n"
            ),
        ),
        bagRoot = bagRoot,
        bagInfo = bagInfo
      )

      storeBagContents(bagContents)

      val bag = withBagReader {
        _.get(bagRoot).value
      }

      bag.manifest shouldBe NewPayloadManifest(
        entries = Map(
          BagPath("data/README.txt") -> MultiChecksumValue(
            md5 = Some(ChecksumValue("a86e2699931d4f3d1456e79383749e43")),
            sha1 = None,
            sha256 = None,
            sha512 = Some(ChecksumValue("7cd31c95fc5a40e5be7bf46e84df52c6d8d50e9003dfb7e3b85ac9c704b90a63ac220147645ff22d410166356133d241a7346e452c863601ce68b82d075031f8"))
          )
        )
      )
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
        _.get(bagRoot).value.fetch shouldBe None
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

  private def createBag()(namespace: Namespace, bucket: Bucket)(
    implicit typedStore: TypedStore[BagLocation, String]
  ): (BagPrefix, BagInfo) = {
    val bagContents = createBagContentsWith()(namespace, bucket)

    storeBagContents(bagContents)

    (bagContents.bagRoot, bagContents.bagInfo)
  }
}
