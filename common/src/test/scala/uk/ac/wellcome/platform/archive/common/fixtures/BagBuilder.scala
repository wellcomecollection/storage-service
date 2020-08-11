package uk.ac.wellcome.platform.archive.common.fixtures

import java.security.MessageDigest

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.bagit.models._
import uk.ac.wellcome.platform.archive.common.generators.{
  BagInfoGenerators,
  StorageSpaceGenerators
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.s3.S3ObjectLocation
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.store.s3.S3TypedStore
import uk.ac.wellcome.storage.{Location, Prefix}

import scala.util.Random

case class PayloadEntry(bagPath: BagPath, path: String, contents: String)

trait BagBuilder[BagLocation <: Location, BagPrefix <: Prefix[BagLocation], Namespace]
    extends StorageSpaceGenerators
    with BagInfoGenerators {

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
    version: BagVersion
  )(
    implicit namespace: Namespace
  ): BagPrefix

  def createBagLocation(bagRoot: BagPrefix, path: String): BagLocation

  def uploadBagObjects(
    bagRoot: BagPrefix,
    objects: Map[BagLocation, String],
    fetchObjects: Map[S3ObjectLocation, String] = Map.empty
  )(
    implicit typedStore: TypedStore[BagLocation, String],
    s3Client: AmazonS3
  ): Unit = {
    objects.foreach {
      case (location, contents) =>
        typedStore.put(location)(contents) shouldBe a[Right[_, _]]
    }
    fetchObjects.foreach {
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
    implicit namespace: Namespace,
    primaryBucket: Bucket
  ): BagContents = {
    val fetchEntryCount = getFetchEntryCount(payloadFileCount)

    val payloadFiles = createPayloadFiles(
      space = space,
      externalIdentifier = externalIdentifier,
      version = version,
      payloadFileCount = payloadFileCount - fetchEntryCount
    )

    val fetchEntries = createPayloadFiles(
      space = space,
      externalIdentifier = externalIdentifier,
      version = version.copy(underlying = version.underlying - 1),
      payloadFileCount = fetchEntryCount
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

    val bagItFile =
      ManifestFile(
        name = s"bagit.txt",
        contents = """
                     |BagIt-Version: 0.97
                     |Tag-File-Character-Encoding: UTF-8
                   """.stripMargin.trim
      )

    val bagInfoFile =
      createBagInfo(bagInfo)
        .map { contents =>
          ManifestFile(
            name = s"bag-info.txt",
            contents = contents
          )
        }

    val fetchFile = createFetchFile(primaryBucket.name, fetchEntries)
      .map { contents =>
        ManifestFile(
          name = "fetch.txt",
          contents = contents
        )
      }

    val tagManifestFiles: List[ManifestFile] =
      payloadManifest.toList ++
        bagInfoFile.toList ++
        fetchFile.toList ++ List(bagItFile)

    val tagManifest = createTagManifest(tagManifestFiles)
      .map { contents =>
        ManifestFile(
          name = "tagmanifest-sha256.txt",
          contents = contents
        )
      }

    val bagRoot = createBagRoot(space, externalIdentifier, version)

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
    primaryBucketName: String,
    entries: Seq[PayloadEntry]
  ): Option[String] =
    if (entries.isEmpty) {
      None
    } else {
      Some(
        entries
          .map { entry =>
            buildFetchEntryLine(primaryBucketName, entry)
          }
          .mkString("\n")
      )
    }

  protected def buildFetchEntryLine(
    primaryBucketName: String,
    entry: PayloadEntry
  ): String = {
    val displaySize =
      if (Random.nextBoolean()) entry.contents.getBytes.length.toString else "-"

    s"""s3://${primaryBucketName}/${entry.path} $displaySize ${entry.bagPath}"""
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

  private def createPayloadFiles(
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: BagVersion,
    payloadFileCount: Int
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

  private def randomPath: String =
    (1 to randomInt(from = 1, to = 5))
      .map { _ =>
        randomAlphanumeric
      }
      .mkString("/")

  private def createDigest(string: String): String =
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
