package uk.ac.wellcome.platform.archive.bagverifier.services

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, EitherValues, OptionValues, TryValues}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.fixity.{FailedChecksumNoMatch, FileFixityCorrect}
import uk.ac.wellcome.platform.archive.bagverifier.models._
import uk.ac.wellcome.platform.archive.bagverifier.storage.LocationNotFound
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagPath, BagVersion, ExternalIdentifier, PayloadOxum}
import uk.ac.wellcome.platform.archive.common.bagit.services.{BagReader, BagUnavailable}
import uk.ac.wellcome.platform.archive.common.fixtures.{BagBuilder, PayloadEntry}
import uk.ac.wellcome.platform.archive.common.generators.{BagInfoGenerators, StorageSpaceGenerators}
import uk.ac.wellcome.platform.archive.common.storage.models.{IngestFailed, IngestStepResult, IngestStepSucceeded, StorageSpace}
import uk.ac.wellcome.storage.s3.S3ObjectLocation
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.store.fixtures.NamespaceFixtures
import uk.ac.wellcome.storage.{Location, Prefix}

import scala.util.Try

trait StandaloneBagVerifierTestCases[
  BagLocation <: Location,
  BagPrefix <: Prefix[
    BagLocation
  ],
  Namespace
] extends BagVerifierTestCases[
      StandaloneBagVerifier[BagLocation, BagPrefix],
      StandaloneBagVerifyContext[BagPrefix],
      BagLocation,
      BagPrefix,
      Namespace
    ] {
  def withBagContext[R](
    bagRoot: BagPrefix
  )(testWith: TestWith[StandaloneBagVerifyContext[BagPrefix], R]): R =
    testWith(
      StandaloneBagVerifyContext(bagRoot)
    )
}

trait BagVerifierTestCases[Verifier <: BagVerifier[
  BagContext,
  BagLocation,
  BagPrefix
], BagContext <: BagVerifyContext[BagPrefix], BagLocation <: Location, BagPrefix <: Prefix[
  BagLocation
], Namespace]
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with OptionValues
    with TryValues
    with StorageSpaceGenerators
    with BagInfoGenerators
    with NamespaceFixtures[BagLocation, Namespace] {

  def withTypedStore[R](
    testWith: TestWith[TypedStore[BagLocation, String], R]
  ): R

  def withVerifier[R](namespace: Namespace)(
    testWith: TestWith[Verifier, R]
  )(implicit typedStore: TypedStore[BagLocation, String]): R

  def withBagContext[R](bagRoot: BagPrefix)(
    testWith: TestWith[BagContext, R]
  ): R
  val replicaBagBuilder: BagBuilder[
    BagLocation,
    BagPrefix,
    Namespace
  ]
  val payloadFileCount: Int = randomInt(from = 1, to = 10)

  val expectedFileCount: Int = payloadFileCount + List(
    "manifest-sha256.txt",
    "bagit.txt",
    "bag-info.txt"
  ).size

  def writeFile(
    location: BagLocation,
    contents: String = randomAlphanumeric
  ): Unit

  def createBagReader: BagReader[BagLocation, BagPrefix]

  trait BagBuilderImpl extends BagBuilder[BagLocation, BagPrefix, Namespace] {
    override def createBagRoot(
      space: StorageSpace,
      externalIdentifier: ExternalIdentifier,
      version: BagVersion
    )(
      implicit namespace: Namespace
    ): BagPrefix =
      replicaBagBuilder.createBagRoot(space, externalIdentifier, version)

    override def createBagLocation(
      bagRoot: BagPrefix,
      path: String
    ): BagLocation =
      replicaBagBuilder.createBagLocation(bagRoot, path)

    override def buildFetchEntryLine(
      entry: PayloadEntry
    )(implicit namespace: Namespace): String =
      replicaBagBuilder.buildFetchEntryLine(entry)
  }

  it("passes a bag with correct checksum values") {
    withNamespace { implicit namespace =>
      withTypedStore { implicit typedStore =>
        val space = createStorageSpace

        val (bagObjects, bagRoot, bagInfo) =
          replicaBagBuilder.createBagContentsWith(
            space = space,
            payloadFileCount = payloadFileCount
          )
        replicaBagBuilder.uploadBagObjects(
          bagRoot = bagRoot,
          objects = bagObjects
        )

        val ingestStep =
          withBagContext(bagRoot) { bagContext =>
            withVerifier(namespace) {
              _.verify(
                ingestId = createIngestID,
                bagContext = bagContext,
                space = space,
                externalIdentifier = bagInfo.externalIdentifier
              )
            }
          }

        val result = ingestStep.success.get

        result shouldBe a[IngestStepSucceeded[_]]
        result.summary shouldBe a[VerificationSuccessSummary]

        val summary = result.summary
          .asInstanceOf[VerificationSuccessSummary]
        val fixityListResult = summary.fixityListResult.value

        verifySuccessCount(
          fixityListResult.locations,
          expectedCount = expectedFileCount
        )
      }
    }
  }

  it("fails a bag with an incorrect checksum in the file manifest") {
    val badBuilder: BagBuilderImpl = new BagBuilderImpl {
      override protected def createPayloadManifest(
        entries: Seq[PayloadEntry]
      ): Option[String] =
        super.createPayloadManifest(
          entries.head.copy(contents = randomAlphanumeric) +: entries.tail
        )
    }

    assertBagFails(badBuilder) {
      case (_, summary) =>
        val fixityListResult = summary.fixityListResult.value

        verifySuccessCount(
          fixityListResult.correct,
          expectedCount = expectedFileCount - 1
        )
        fixityListResult.errors should have size 1

        val fixityError = fixityListResult.errors.head
        val error = fixityError.e

        error shouldBe a[FailedChecksumNoMatch]
        error.getMessage should include("Checksum values do not match!")
    }
  }

  it("fails a bag with multiple incorrect checksums in the file manifest") {
    val badBuilder = new BagBuilderImpl {
      override protected def createPayloadManifest(
        entries: Seq[PayloadEntry]
      ): Option[String] =
        super.createPayloadManifest(
          entries.map { _.copy(contents = randomAlphanumeric) }
        )
    }

    assertBagFails(badBuilder) {
      case (ingestFailed, _) =>
        ingestFailed.maybeUserFacingMessage.get should startWith(
          s"Unable to verify $payloadFileCount files in the bag:"
        )
    }
  }

  it("fails a bag if the file manifest refers to a non-existent file") {
    val badBuilder = new BagBuilderImpl {
      override protected def createPayloadManifest(
        entries: Seq[PayloadEntry]
      ): Option[String] =
        super.createPayloadManifest(
          entries.tail :+ PayloadEntry(
            bagPath = BagPath(randomAlphanumeric),
            path = randomAlphanumeric,
            contents = randomAlphanumeric
          )
        )

      // This ensures that the fetch file won't refer to the entry
      // we've deleted from the manifest.
      override protected def getFetchEntryCount(payloadFileCount: Int): Int = 0
    }

    assertBagFails(badBuilder) {
      case (_, summary) =>
        val fixityListResult = summary.fixityListResult.value

        verifySuccessCount(
          fixityListResult.correct,
          expectedCount = expectedFileCount - 1
        )
        fixityListResult.errors should have size 1

        val fixityError = fixityListResult.errors.head
        val error = fixityError.e

        error shouldBe a[LocationNotFound[_]]
        error.getMessage should startWith("Location not available!")
    }
  }

  it("fails a bag if the file manifest does not exist") {
    val badBuilder = new BagBuilderImpl {
      override protected def createPayloadManifest(
        entries: Seq[PayloadEntry]
      ): Option[String] =
        None
    }

    assertBagIncomplete(badBuilder) {
      case (ingestFailed, summary) =>
        val error = summary.e

        error shouldBe a[BagUnavailable]
        error.getMessage should include("Error loading manifest-sha256.txt")

        ingestFailed.maybeUserFacingMessage.get shouldBe "Error loading manifest-sha256.txt: no such file!"
    }
  }

  it("fails a bag if the tag manifest does not exist") {
    val badBuilder = new BagBuilderImpl {
      override protected def createTagManifest(
        entries: Seq[ManifestFile]
      ): Option[String] =
        None
    }

    assertBagIncomplete(badBuilder) {
      case (ingestFailed, summary) =>
        val error = summary.e

        error shouldBe a[BagUnavailable]
        error.getMessage should include("Error loading tagmanifest-sha256.txt")

        ingestFailed.maybeUserFacingMessage.get shouldBe "Error loading tagmanifest-sha256.txt: no such file!"
    }
  }

  it("fails if the external identifier in the bag-info.txt is incorrect") {
    val space = createStorageSpace
    val externalIdentifier = randomAlphanumeric
    val bagInfoExternalIdentifier =
      ExternalIdentifier(externalIdentifier + "_bag-info")
    val payloadExternalIdentifier =
      ExternalIdentifier(externalIdentifier + "_payload")

    withNamespace { implicit namespace =>
      withTypedStore { implicit typedStore =>
        val (bagObjects, bagRoot, _) = replicaBagBuilder.createBagContentsWith(
          externalIdentifier = bagInfoExternalIdentifier
        )
        replicaBagBuilder.uploadBagObjects(
          bagRoot = bagRoot,
          objects = bagObjects
        )

        val ingestStep =
          withBagContext(bagRoot) { bagContext =>
            withVerifier(namespace) {
              _.verify(
                ingestId = createIngestID,
                bagContext = bagContext,
                space = space,
                externalIdentifier = payloadExternalIdentifier
              )
            }
          }

        val result = ingestStep.success.get

        result shouldBe a[IngestFailed[_]]
        result.summary shouldBe a[VerificationIncompleteSummary]

        result.maybeUserFacingMessage.get should startWith(
          "External identifier in bag-info.txt does not match request"
        )
      }
    }
  }

  describe("checks the fetch file") {
    it("fails if the fetch file refers to a file not in the manifest") {
      val badBuilder = new BagBuilderImpl {
        override protected def createFetchFile(
          entries: Seq[PayloadEntry]
        )(implicit namespace: Namespace): Option[String] =
          super.createFetchFile(
            entries :+ entries.head.copy(
              bagPath = BagPath(entries.head.bagPath + "_extra"),
              path = entries.head.path + "_extra",
              contents = randomAlphanumeric
            )
          )
      }

      assertBagIncomplete(badBuilder) {
        case (ingestFailed, _) =>
          ingestFailed.e.getMessage should startWith(
            "fetch.txt refers to paths that aren't in the bag manifest: "
          )

          ingestFailed.maybeUserFacingMessage.get should startWith(
            "fetch.txt refers to paths that aren't in the bag manifest: "
          )
      }
    }

    it("fails if the fetch file refers to a file with the wrong URI scheme") {
      val wrongSchemeBuilder = new BagBuilderImpl {
        override def buildFetchEntryLine(
          entry: PayloadEntry
        )(implicit namespace: Namespace): String =
          super.buildFetchEntryLine(entry).replace("s3://", "none://")

        override protected def getFetchEntryCount(payloadFileCount: Int): Int =
          payloadFileCount
      }

      assertBagIncomplete(wrongSchemeBuilder) {
        case (ingestFailed, _) =>
          ingestFailed.e.getMessage should startWith(
            "fetch.txt refers to paths in a mismatched prefix or with a non-S3 URI scheme:"
          )

          ingestFailed.maybeUserFacingMessage.get should startWith(
            "fetch.txt refers to paths in a mismatched prefix or with a non-S3 URI scheme:"
          )
      }
    }

    it("fails if the fetch file refers to a file in a different namespace") {
      withNamespace { wrongNamespace =>
        val wrongBucketFetchBuilder = new BagBuilderImpl {
          override def buildFetchEntryLine(
            entry: PayloadEntry
          )(implicit namespace: Namespace): String =
            super.buildFetchEntryLine(entry)(namespace = wrongNamespace)

          override protected def getFetchEntryCount(
            payloadFileCount: Int
          ): Int =
            payloadFileCount
        }

        assertBagIncomplete(wrongBucketFetchBuilder) {
          case (ingestFailed, _) =>
            ingestFailed.e.getMessage should startWith(
              "fetch.txt refers to paths in a mismatched prefix or with a non-S3 URI scheme:"
            )

            ingestFailed.maybeUserFacingMessage.get should startWith(
              "fetch.txt refers to paths in a mismatched prefix or with a non-S3 URI scheme:"
            )
        }
      }
    }

    it("fails if the fetch file refers to a file in the wrong space") {
      val bagSpaceFetchBuilder = new BagBuilderImpl {
        override def buildFetchEntryLine(
          entry: PayloadEntry
        )(implicit namespace: Namespace): String =
          super.buildFetchEntryLine(
            entry.copy(
              path = "badspace_" + entry.path
            )
          )

        override protected def getFetchEntryCount(payloadFileCount: Int): Int =
          payloadFileCount
      }

      assertBagIncomplete(bagSpaceFetchBuilder) {
        case (ingestFailed, _) =>
          ingestFailed.e.getMessage should startWith(
            "fetch.txt refers to paths in a mismatched prefix or with a non-S3 URI scheme:"
          )

          ingestFailed.maybeUserFacingMessage.get should startWith(
            "fetch.txt refers to paths in a mismatched prefix or with a non-S3 URI scheme:"
          )
      }
    }

    it(
      "fails if the fetch file refers to a file with the wrong external identifier"
    ) {
      val badExternalIdentifierFetchBuilder = new BagBuilderImpl {
        override def buildFetchEntryLine(
          entry: PayloadEntry
        )(implicit namespace: Namespace): String =
          super.buildFetchEntryLine(
            entry.copy(
              path = entry.path.replace("/", "/wrong_")
            )
          )

        override protected def getFetchEntryCount(payloadFileCount: Int): Int =
          payloadFileCount
      }

      assertBagIncomplete(badExternalIdentifierFetchBuilder) {
        case (ingestFailed, _) =>
          ingestFailed.e.getMessage should startWith(
            "fetch.txt refers to paths in a mismatched prefix or with a non-S3 URI scheme:"
          )

          ingestFailed.maybeUserFacingMessage.get should startWith(
            "fetch.txt refers to paths in a mismatched prefix or with a non-S3 URI scheme:"
          )
      }
    }
  }

  describe("checks for unreferenced files") {
    it("fails if there is one unreferenced file") {
      val badBuilder = new BagBuilderImpl {
        override def uploadBagObjects(
          bagRoot: BagPrefix,
          bagObjects: Map[BagLocation, String]
        )(implicit typedStore: TypedStore[BagLocation, String]): Unit = {
          super.uploadBagObjects(bagRoot = bagRoot, objects = bagObjects)

          val location = bagRoot.asLocation("unreferencedfile.txt")
          writeFile(location)
        }
      }

      assertBagIncomplete(badBuilder) {
        case (ingestFailed, _) =>
          ingestFailed.e.getMessage should startWith(
            "Bag contains a file which is not referenced in the manifest:"
          )

          ingestFailed.maybeUserFacingMessage.get shouldBe
            "Bag contains a file which is not referenced in the manifest: /unreferencedfile.txt"
      }
    }

    it("fails if there are multiple unreferenced files") {
      val badBuilder = new BagBuilderImpl {
        override def uploadBagObjects(
          bagRoot: BagPrefix,
          bagObjects: Map[BagLocation, String]
        )(implicit typedStore: TypedStore[BagLocation, String]): Unit = {
          super.uploadBagObjects(bagRoot = bagRoot, objects = bagObjects)

          (1 to 3).foreach { i =>
            val location = bagRoot.asLocation(s"unreferencedfile_$i.txt")
            writeFile(location)
          }
        }
      }

      assertBagIncomplete(badBuilder) {
        case (ingestFailed, _) =>
          ingestFailed.e.getMessage should startWith(
            "Bag contains 3 files which are not referenced in the manifest:"
          )

          ingestFailed.maybeUserFacingMessage.get shouldBe
            s"Bag contains 3 files which are not referenced in the manifest: " +
              "/unreferencedfile_1.txt, /unreferencedfile_2.txt, /unreferencedfile_3.txt"
      }
    }

    it("fails if a file in the fetch.txt also appears in the bag") {
      val alwaysWriteAsFetchBuilder = new BagBuilderImpl {
        override def uploadBagObjects(
          bagRoot: BagPrefix,
          bagObjects: Map[BagLocation, String]
        )(implicit typedStore: TypedStore[BagLocation, String]): Unit = {
          super.uploadBagObjects(bagRoot = bagRoot, objects = bagObjects)

          val bag = createBagReader.get(bagRoot).right.value

          // Write one of the fetch.txt entries as a concrete file
          val badFetchPath: BagPath = bag.fetch.get.paths.head
          val badFetchLocation = bagRoot.asLocation(badFetchPath.value)
          writeFile(badFetchLocation)
        }
      }

      assertBagIncomplete(alwaysWriteAsFetchBuilder) {
        case (ingestFailed, _) =>
          ingestFailed.e.getMessage should startWith(
            "Files referred to in the fetch.txt also appear in the bag:"
          )

          ingestFailed.maybeUserFacingMessage.get should startWith(
            "Files referred to in the fetch.txt also appear in the bag:"
          )
      }
    }

    it("passes a bag that includes an extra manifest/tag manifest") {
      withNamespace { implicit namespace =>
        withTypedStore { implicit typedStore =>
          val space = createStorageSpace

          val (bagObjects, bagRoot, bagInfo) =
            replicaBagBuilder.createBagContentsWith(space = space)
          replicaBagBuilder.uploadBagObjects(
            bagRoot = bagRoot,
            objects = bagObjects
          )

          val location = bagRoot.asLocation("tagmanifest-sha512.txt")
          writeFile(location)

          val ingestStep =
            withBagContext(bagRoot) { bagContext =>
              withVerifier(namespace) {
                _.verify(
                  ingestId = createIngestID,
                  bagContext = bagContext,
                  space = space,
                  externalIdentifier = bagInfo.externalIdentifier
                )
              }
            }

          ingestStep.success.get shouldBe a[IngestStepSucceeded[_]]
        }
      }
    }
  }

  describe("checks the Payload-Oxum") {
    it("fails if the Payload-Oxum has the wrong file count") {
      val badBuilder = new BagBuilderImpl {
        override protected def createPayloadOxum(
          entries: Seq[PayloadEntry]
        ): PayloadOxum = {
          val oxum = super.createPayloadOxum(entries)

          oxum.copy(numberOfPayloadFiles = oxum.numberOfPayloadFiles - 1)
        }
      }

      assertBagIncomplete(badBuilder) {
        case (ingestFailed, _) =>
          ingestFailed.maybeUserFacingMessage.get shouldBe
            s"""Payload-Oxum has the wrong number of payload files: ${payloadFileCount - 1}, but bag manifest has $payloadFileCount"""
      }
    }

    it("fails if the Payload-Oxum has the wrong octet count") {
      val badBuilder = new BagBuilderImpl {
        override protected def createPayloadOxum(
          entries: Seq[PayloadEntry]
        ): PayloadOxum = {
          val oxum = super.createPayloadOxum(entries)

          oxum.copy(payloadBytes = oxum.payloadBytes - 1)
        }
      }

      assertBagIncomplete(badBuilder) {
        case (ingestFailed, _) =>
          ingestFailed.maybeUserFacingMessage.get should fullyMatch regex
            s"""Payload-Oxum has the wrong octetstream sum: \\d+ bytes, but bag actually contains \\d+ bytes"""
      }
    }
  }

  private def verifySuccessCount(
    successes: List[FileFixityCorrect[_]],
    expectedCount: Int
  ): Assertion =
    if (successes
          .map { _.objectLocation.asInstanceOf[S3ObjectLocation].key }
          .exists {
            _.endsWith("/fetch.txt")
          }) {
      successes.size shouldBe expectedCount + 1
    } else {
      successes.size shouldBe expectedCount
    }

  private def assertBagFails(badBuilder: BagBuilderImpl)(
    assertion: (
      IngestFailed[VerificationFailureSummary],
      VerificationFailureSummary
    ) => Assertion
  ): Assertion =
    assertBagResultFails(badBuilder) { result =>
      result.summary shouldBe a[VerificationFailureSummary]

      val failedResult =
        result.asInstanceOf[IngestFailed[VerificationFailureSummary]]
      val summary = result.summary.asInstanceOf[VerificationFailureSummary]

      assertion(failedResult, summary)
    }

  // Given a builder that fails to create a valid bag for some reason, ensure that
  // it is caught correctly by the verifier.
  private def assertBagResultFails(
    badBuilder: BagBuilderImpl
  )(assertion: IngestStepResult[VerificationSummary] => Assertion): Assertion =
    withNamespace { implicit namespace =>
      withTypedStore { implicit typedStore =>
        val space = createStorageSpace
        val (bagObjects, bagRoot, bagInfo) = badBuilder.createBagContentsWith(
          space = space,
          payloadFileCount = payloadFileCount
        )
        badBuilder.uploadBagObjects(bagRoot, objects = bagObjects)

        val ingestStep =
          withBagContext(bagRoot) { bagContext =>
            withVerifier(namespace) {
              _.verify(
                ingestId = createIngestID,
                bagContext = bagContext,
                space = space,
                externalIdentifier = bagInfo.externalIdentifier
              )
            }
          }

        val result = ingestStep.success.get
        debug(s"result = $result")

        result shouldBe a[IngestFailed[_]]
        assertion(result)
      }
    }

  private def assertBagIncomplete(badBuilder: BagBuilderImpl)(
    assertion: (
      IngestFailed[VerificationIncompleteSummary],
      VerificationIncompleteSummary
    ) => Assertion
  ): Assertion =
    assertBagResultFails(badBuilder) { result =>
      result.summary shouldBe a[VerificationIncompleteSummary]

      val failedResult =
        result.asInstanceOf[IngestFailed[VerificationIncompleteSummary]]
      val summary = result.summary.asInstanceOf[VerificationIncompleteSummary]

      assertion(failedResult, summary)
    }
}

trait ReplicatedBagVerifierTestCases[
  SrcBagLocation <: Location,
  SrcBagPrefix <: Prefix[SrcBagLocation],
  SrcNamespace,
  ReplicaBagLocation <: Location,
  ReplicaBagPrefix <: Prefix[ReplicaBagLocation],
  ReplicaNamespace
] extends BagVerifierTestCases[
      ReplicatedBagVerifier[
        SrcBagLocation,
        SrcBagPrefix,
        ReplicaBagLocation,
        ReplicaBagPrefix
      ],
      ReplicatedBagVerifyContext[SrcBagPrefix, ReplicaBagPrefix],
      ReplicaBagLocation,
      ReplicaBagPrefix,
      ReplicaNamespace
    ] {

  protected def copyTagManifest(
    srcRoot: SrcBagPrefix,
    replicaRoot: ReplicaBagPrefix
  ): Unit

  def createSrcPrefix(implicit namespace: SrcNamespace): SrcBagPrefix

  def withSrcNamespace[R](testWith: TestWith[SrcNamespace, R]): R
  def withReplicaNamespace[R](testWith: TestWith[ReplicaNamespace, R]): R

  def withSrcTypedStore[R](
    testWith: TestWith[TypedStore[SrcBagLocation, String], R]
  ): R
  def withReplicaTypedStore[R](
    testWith: TestWith[TypedStore[ReplicaBagLocation, String], R]
  ): R

  val srcBagBuilder: BagBuilder[SrcBagLocation, SrcBagPrefix, SrcNamespace]

  def withNamespace[R](testWith: TestWith[ReplicaNamespace, R]): R =
    withReplicaNamespace { namespace =>
      testWith(namespace)
    }

  def withTypedStore[R](
    testWith: TestWith[TypedStore[ReplicaBagLocation, String], R]
  ): R =
    withReplicaTypedStore { typedStore =>
      testWith(typedStore)
    }

  override def withBagContext[R](replicaRoot: ReplicaBagPrefix)(
    testWith: TestWith[
      ReplicatedBagVerifyContext[SrcBagPrefix, ReplicaBagPrefix],
      R
    ]
  ): R =
    withSrcNamespace { implicit srcNamespace =>
      val srcRoot = createSrcPrefix

      // To keep the standalone verifier tests happy, copy the tagmanifest-sha256.txt into
      // a newly-created srcPrefix.  Note: in at least one test, this file is deliberately
      // missing, but we shouldn't throw.
      Try { copyTagManifest(srcRoot, replicaRoot) }

      testWith(
        ReplicatedBagVerifyContext(
          srcRoot = srcRoot,
          replicaRoot = replicaRoot
        )
      )
    }

  it("fails a bag if it doesn't match original tag manifest") {
    withSrcNamespace { implicit srcNamespace =>
      withReplicaNamespace { implicit replicaNamespace =>
        withSrcTypedStore { implicit srcTypedStore =>
          withReplicaTypedStore { implicit replicaTypedStore =>
            val space = createStorageSpace

            val (srcObjects, srcRoot, _) =
              srcBagBuilder.createBagContentsWith(space = space)
            srcBagBuilder.uploadBagObjects(srcRoot, objects = srcObjects)

            val (replicaObjects, replicaRoot, bagInfo) =
              replicaBagBuilder.createBagContentsWith(space = space)
            replicaBagBuilder.uploadBagObjects(
              replicaRoot,
              objects = replicaObjects
            )

            val ingestStep =
              withVerifier(replicaNamespace) {
                _.verify(
                  ingestId = createIngestID,
                  bagContext = ReplicatedBagVerifyContext(
                    srcRoot = srcRoot,
                    replicaRoot = replicaRoot
                  ),
                  space = space,
                  externalIdentifier = bagInfo.externalIdentifier
                )
              }

            val result = ingestStep.success.get

            result shouldBe a[IngestFailed[_]]
            result.summary shouldBe a[VerificationIncompleteSummary]

            result.maybeUserFacingMessage shouldNot be(defined)
          }
        }
      }
    }
  }

  it("fails a bag if it cannot read the original bag") {
    withSrcNamespace { implicit srcNamespace =>
      withReplicaNamespace { implicit replicaNamespace =>
        withReplicaTypedStore { implicit replicaTypedStore =>
          val space = createStorageSpace

          val (bagObjects, bagRoot, bagInfo) =
            replicaBagBuilder.createBagContentsWith(space = space)
          replicaBagBuilder.uploadBagObjects(bagRoot, objects = bagObjects)

          val srcRoot = createSrcPrefix

          val ingestStep =
            withVerifier(replicaNamespace) {
              _.verify(
                ingestId = createIngestID,
                bagContext = ReplicatedBagVerifyContext(
                  srcRoot = srcRoot,
                  replicaRoot = bagRoot
                ),
                space = space,
                externalIdentifier = bagInfo.externalIdentifier
              )
            }

          val result = ingestStep.success.get

          result shouldBe a[IngestFailed[_]]
          result.summary shouldBe a[VerificationIncompleteSummary]

          result.maybeUserFacingMessage shouldNot be(defined)
        }
      }
    }
  }
}
