package weco.storage_service.fixtures

import java.security.MessageDigest
import weco.storage_service.bagit.models._
import weco.storage.fixtures.S3Fixtures
import weco.storage_service.storage.models.StorageSpace
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.providers.s3.S3ObjectLocation
import weco.storage.store.TypedStore
import weco.storage.store.s3.S3TypedStore
import weco.storage.{Location, Prefix}
import weco.storage_service.bagit.models.BagPath
import weco.storage_service.generators.{
  BagInfoGenerators,
  StorageSpaceGenerators
}

import scala.util.Random

case class PayloadEntry(bagPath: BagPath, path: String, contents: String)

/** This is used to build example bags for testing.
  *
  * It exposes a bunch of protected methods that control different bits of the
  * bag creation process, so you can simulate different mistakes when creating
  * the bag.  This is particularly useful for testing the bag verifier.
  */
trait BagBuilder[BagLocation <: Location, BagPrefix <: Prefix[BagLocation], Namespace]
    extends StorageSpaceGenerators
    with BagInfoGenerators
    with S3Fixtures {

  implicit val typedStore: TypedStore[BagLocation, String]

  case class BagContents(
    fetchObjects: Map[S3ObjectLocation, String],
    bagObjects: Map[BagLocation, String],
    bagRoot: BagPrefix,
    bagInfo: BagInfo
  )

  case class ManifestFile(name: String, contents: String)

  def createBagRoot(
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: BagVersion = createBagVersion
  )(namespace: Namespace): BagPrefix

  def createBagLocation(bagRoot: BagPrefix, path: String): BagLocation

  def storeBagWith(
    space: StorageSpace = createStorageSpace,
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    version: BagVersion = BagVersion(randomInt(from = 2, to = 10)),
    payloadFileCount: Int = randomInt(from = 5, to = 50)
  )(
    implicit namespace: Namespace,
    primaryBucket: Bucket
  ): (BagPrefix, BagInfo) = {

    val bagContents = createBagContentsWith(
      space = space,
      externalIdentifier = externalIdentifier,
      version = version,
      payloadFileCount = payloadFileCount
    )(namespace, primaryBucket)

    storeBagContents(bagContents)

    (bagContents.bagRoot, bagContents.bagInfo)
  }

  def storeBagContents(bagContents: BagContents)(
    implicit typedStore: TypedStore[BagLocation, String]
  ): Unit = {
    bagContents.bagObjects.foreach {
      case (location, contents) =>
        typedStore.put(location)(contents) shouldBe a[Right[_, _]]
    }
    bagContents.fetchObjects.foreach {
      case (fetchObjectLocation, fetchObjectContents) =>
        S3TypedStore[String].put(fetchObjectLocation)(fetchObjectContents) shouldBe a[
          Right[_, _]
        ]
    }
  }

  protected def getFetchEntryCount(payloadFileCount: Int): Int =
    randomInt(from = 0, to = payloadFileCount)

  def createBagContentsWith(
    space: StorageSpace = createStorageSpace,
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    version: BagVersion = BagVersion(randomInt(from = 2, to = 10)),
    payloadFileCount: Int = randomInt(from = 5, to = 50)
  )(
    namespace: Namespace,
    primaryBucket: Bucket
  ): BagContents = {
    val fetchEntryCount = getFetchEntryCount(payloadFileCount)

    val payloadFiles = createPayloadFiles(
      space = space,
      externalIdentifier = externalIdentifier,
      version = version,
      payloadFileCount = payloadFileCount - fetchEntryCount,
      isFetch = false
    )

    val fetchEntries = createPayloadFiles(
      space = space,
      externalIdentifier = externalIdentifier,
      version = version.copy(underlying = version.underlying - 1),
      payloadFileCount = fetchEntryCount,
      isFetch = true
    )

    val payloadManifest =
      createPayloadManifest(payloadFiles ++ fetchEntries)
        .map { contents =>
          ManifestFile(
            name = s"manifest-sha256.txt",
            contents = contents
          )
        }

    val bagInfo = createBagInfoWith(
      payloadOxum = createPayloadOxum(payloadFiles ++ fetchEntries),
      externalIdentifier = externalIdentifier
    )

    val bagItFile = Seq(createBagDeclaration).flatten

    val bagInfoFile =
      createBagInfo(bagInfo)
        .map { contents =>
          ManifestFile(
            name = s"bag-info.txt",
            contents = contents
          )
        }

    val fetchFile = createFetchFile(primaryBucket, fetchEntries)
      .map { contents =>
        ManifestFile(
          name = "fetch.txt",
          contents = contents
        )
      }

    val tagManifestFiles: List[ManifestFile] =
      payloadManifest.toList ++
        bagInfoFile.toList ++
        fetchFile.toList ++ bagItFile

    val tagManifest = createTagManifest(tagManifestFiles)
      .map { contents =>
        ManifestFile(
          name = "tagmanifest-sha256.txt",
          contents = contents
        )
      }

    val bagRoot = createBagRoot(space, externalIdentifier, version)(namespace)

    val manifestObjects =
      (tagManifestFiles ++ tagManifest.toList).map { manifestFile =>
        bagRoot.asLocation(manifestFile.name) -> manifestFile.contents
      }.toMap

    val payloadObjects =
      payloadFiles.map { payloadEntry =>
        createBagLocation(bagRoot, path = payloadEntry.path) -> payloadEntry.contents
      }.toMap

    val fetchObjects = fetchEntries.map { fetchEntry =>
      S3ObjectLocation(primaryBucket.name, fetchEntry.path) -> fetchEntry.contents
    }.toMap

    BagContents(
      fetchObjects,
      manifestObjects ++ payloadObjects,
      bagRoot,
      bagInfo
    )
  }

  protected def createFetchFile(
    primaryBucket: Bucket,
    entries: Seq[PayloadEntry]
  ): Option[String] =
    if (entries.isEmpty) {
      None
    } else {
      Some(
        entries
          .map { entry =>
            buildFetchEntryLine(primaryBucket, entry)
          }
          .mkString("\n")
      )
    }

  protected def buildFetchEntryLine(
    primaryBucket: Bucket,
    entry: PayloadEntry
  ): String = {
    val displaySize =
      if (Random.nextBoolean()) entry.contents.getBytes.length.toString else "-"

    s"""s3://${primaryBucket.name}/${entry.path} $displaySize ${entry.bagPath}"""
  }

  protected def createPayloadOxum(entries: Seq[PayloadEntry]): PayloadOxum =
    PayloadOxum(
      payloadBytes = entries.map { _.contents.getBytes.length }.sum,
      numberOfPayloadFiles = entries.size
    )

  protected def createPayloadManifest(
    entries: Seq[PayloadEntry]
  ): Option[String] =
    Some(
      createManifest(
        entries.map { entry =>
          (entry.bagPath.value, createDigest(entry.contents))
        }
      )
    )

  protected def createTagManifest(entries: Seq[ManifestFile]): Option[String] =
    Some(
      createManifest(
        entries.map { entry =>
          (entry.name, createDigest(entry.contents))
        }
      )
    )

  protected def createBagDeclaration: Option[ManifestFile] =
    Some(
      ManifestFile(
        name = s"bagit.txt",
        contents = """
                     |BagIt-Version: 0.97
                     |Tag-File-Character-Encoding: UTF-8
                   """.stripMargin.trim
      )
    )

  protected def createBagInfo(bagInfo: BagInfo): Option[String] = {
    def optionalLine[T](maybeValue: Option[T], fieldName: String): String =
      maybeValue.map(value => s"$fieldName: $value").getOrElse("")

    val sourceOrganisationLine =
      optionalLine(bagInfo.sourceOrganisation, "Source-Organization")
    val descriptionLine =
      optionalLine(bagInfo.externalDescription, "External-Description")
    val internalSenderIdentifierLine =
      optionalLine(
        bagInfo.internalSenderIdentifier,
        "Internal-Sender-Identifier"
      )
    val internalSenderDescriptionLine =
      optionalLine(
        bagInfo.internalSenderDescription,
        "Internal-Sender-Description"
      )

    Some(
      s"""External-Identifier: ${bagInfo.externalIdentifier}
         |Payload-Oxum: ${bagInfo.payloadOxum.payloadBytes}.${bagInfo.payloadOxum.numberOfPayloadFiles}
         |Bagging-Date: ${bagInfo.baggingDate.toString}
         |$sourceOrganisationLine
         |$descriptionLine
         |$internalSenderIdentifierLine
         |$internalSenderDescriptionLine
        """.stripMargin.trim
    )
  }

  private def createManifest(entries: Seq[(String, String)]): String =
    entries
      .map { case (name, digest) => s"""$digest  $name""" }
      .mkString("\n")

  protected def createBagRootPath(
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: BagVersion
  ): String =
    // This mimics the structure of bags stored by the replicator
    s"$space/$externalIdentifier/$version"

  protected def createPayloadFiles(
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: BagVersion,
    payloadFileCount: Int,
    isFetch: Boolean
  ): Seq[PayloadEntry] = {
    val bagRoot = createBagRootPath(space, externalIdentifier, version)

    (1 to payloadFileCount).map { _ =>
      val bagPath = BagPath(s"data/$randomPath")
      PayloadEntry(
        bagPath = bagPath,
        path = s"$bagRoot/$bagPath",
        contents = Random.nextString(length = randomInt(1, 256))
      )
    }
  }

  protected def randomPath: String =
    (1 to randomInt(from = 1, to = 5))
      .map { _ =>
        randomAlphanumeric()
      }
      .mkString("/")

  protected def createDigest(string: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(string.getBytes)
      .map(0xFF & _)
      .map {
        "%02x".format(_)
      }
      .foldLeft("") {
        _ + _
      }
}
