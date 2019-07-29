package uk.ac.wellcome.platform.archive.common.fixtures

import java.security.MessageDigest
import java.time.LocalDate

import uk.ac.wellcome.platform.archive.common.bagit.models._
import uk.ac.wellcome.platform.archive.common.generators.BagInfoGenerators

import scala.util.Random

trait BagIt extends BagInfoGenerators {
  private val bagItFileContents = {
    """BagIt-Version: 0.97
      |Tag-File-Character-Encoding: UTF-8
    """.stripMargin.trim
  }

  case class GeneratedBag(
    dataFiles: Seq[FileEntry],
    tagManifestFiles: Seq[FileEntry],
    metaManifest: Option[FileEntry],
    bagInfo: BagInfo
  ) {
    def allFiles: Seq[FileEntry] =
      dataFiles ++ tagManifestFiles ++ metaManifest.toList
  }

  def createBag(
    payloadOxum: Option[PayloadOxum],
    externalIdentifier: ExternalIdentifier,
    dataFileCount: Int = 1,
    createDigest: String => String = createValidDigest,
    createDataManifest: List[(String, String)] => Option[FileEntry] =
      createValidDataManifest,
    createTagManifest: List[(String, String)] => Option[FileEntry] =
      createValidTagManifest,
    createBagItFile: => Option[FileEntry] = createValidBagItFile,
    createBagInfoFile: BagInfo => Option[FileEntry] = createValidBagInfoFile
  ): GeneratedBag = {

    val dataFiles = createDataFiles(dataFileCount)

    val filesAndDigest = dataFiles.map {
      case FileEntry(fileName, contents) => (fileName, createDigest(contents))
    }.toList

    val dataManifest: Option[FileEntry] = createDataManifest(filesAndDigest)

    val maybeBagItFile = createBagItFile

    val bagInfo = createBagInfoWith(
      payloadOxum = payloadOxum.getOrElse(
        PayloadOxum(
          payloadBytes = dataFiles.map { _.contents.getBytes.length }.sum,
          numberOfPayloadFiles = dataFiles.size
        )
      ),
      externalIdentifier = externalIdentifier
    )

    val maybeBagInfoFile = createBagInfoFile(bagInfo)

    val tagManifestFiles = dataManifest.toList ++ maybeBagItFile.toList ++ maybeBagInfoFile.toList

    val tagManifestFileAndDigests = tagManifestFiles.map {
      case FileEntry(fileName, contents) => (fileName, createDigest(contents))
    }

    val metaManifest = createTagManifest(tagManifestFileAndDigests)

    GeneratedBag(dataFiles, tagManifestFiles, metaManifest, bagInfo)
  }

  def createValidBagItFile =
    Some(FileEntry("bagit.txt", bagItFileContents))

  def createValidBagInfoFile(bagInfo: BagInfo) =
    Some(
      FileEntry(
        s"bag-info.txt",
        bagInfoFileContents(
          bagInfo.externalIdentifier,
          bagInfo.payloadOxum,
          bagInfo.baggingDate,
          bagInfo.sourceOrganisation,
          bagInfo.externalDescription,
          bagInfo.internalSenderIdentifier,
          bagInfo.internalSenderDescription
        )
      ))

  def dataManifestWithNonExistingFile(filesAndDigests: Seq[(String, String)]) =
    Some(
      FileEntry(
        name = "manifest-sha256.txt",
        contents = {
          val validContent = filesAndDigests.map {
            case (existingFileName, validFileDigest) =>
              s"""$validFileDigest  $existingFileName"""
          }
          (validContent :+ """invalidchecksum  this/does/not/exists.jpg""")
            .mkString("\n")
        }
      ))

  private def createManifestWithWrongChecksum(filename: String)(
    filesAndDigests: List[(String, String)]) =
    Some(
      FileEntry(
        name = filename,
        contents = {
          filesAndDigests match {
            case (head: (String, String)) :: (list: List[(String, String)]) =>
              val (invalidChecksumFileName, _) = head
              val invalidChecksumManifestEntry =
                s"""badDigest  $invalidChecksumFileName"""
              val validEntries = list.map {
                case (existingFileName, validFileDigest) =>
                  s"""$validFileDigest  $existingFileName"""
              }
              (invalidChecksumManifestEntry +: validEntries).mkString("\n")
            case _ => ""
          }
        }
      ))

  def dataManifestWithWrongChecksum(
    filesAndDigests: List[(String, String)]): Option[FileEntry] =
    createManifestWithWrongChecksum("manifest-sha256.txt")(filesAndDigests)

  def tagManifestWithWrongChecksum(
    filesAndDigests: List[(String, String)]): Option[FileEntry] =
    createManifestWithWrongChecksum("tagmanifest-sha256.txt")(filesAndDigests)

  def createValidDataManifest(
    dataFiles: List[(String, String)]): Option[FileEntry] =
    createValidManifestFile(dataFiles, "manifest-sha256.txt")

  def createValidTagManifest(
    dataFiles: List[(String, String)]): Option[FileEntry] =
    createValidManifestFile(dataFiles, "tagmanifest-sha256.txt")

  private def createValidManifestFile(dataFiles: List[(String, String)],
                                      manifestFileName: String) =
    Some(
      FileEntry(
        s"$manifestFileName",
        dataFiles
          .map {
            case (fileName, contentsDigest) =>
              s"$contentsDigest  $fileName"
          }
          .mkString("\n")
      ))

  def bagInfoFileContents(
    bagIdentifier: ExternalIdentifier,
    payloadOxum: PayloadOxum,
    baggingDate: LocalDate,
    sourceOrganisation: Option[SourceOrganisation] = None,
    externalDescription: Option[ExternalDescription] = None,
    internalSenderIdentifier: Option[InternalSenderIdentifier] = None,
    internalSenderDescription: Option[InternalSenderDescription] = None)
    : String = {
    def optionalLine[T](maybeValue: Option[T], fieldName: String) =
      maybeValue.map(value => s"$fieldName: $value").getOrElse("")

    val sourceOrganisationLine =
      optionalLine(sourceOrganisation, "Source-Organization")
    val descriptionLine =
      optionalLine(externalDescription, "External-Description")
    val internalSenderIdentifierLine =
      optionalLine(internalSenderIdentifier, "Internal-Sender-Identifier")
    val internalSenderDescriptionLine =
      optionalLine(internalSenderDescription, "Internal-Sender-Description")

    s"""External-Identifier: $bagIdentifier
       |Payload-Oxum: ${payloadOxum.payloadBytes}.${payloadOxum.numberOfPayloadFiles}
       |Bagging-Date: ${baggingDate.toString}
       |$sourceOrganisationLine
       |$descriptionLine
       |$internalSenderIdentifierLine
       |$internalSenderDescriptionLine
      """.stripMargin.trim
  }

  private def createDataFiles(dataFileCount: Int) = {
    val subPathLength = Random.nextInt(3)
    val subPathDirectories = (0 to subPathLength).map { i =>
      // Also a replicator tests workaround.  See below.
      // TODO: Fix.
      // randomAlphanumericWithSpace()
      s"dir$i"
    }
    val subPath = subPathDirectories.mkString("/")

    (1 to dataFileCount).map { i =>
      // This is a workaround for a bug in the replicator tests where this
      // creates an ObjectLocation which isn't a valid S3 URI.
      // TODO: Fix it properly, but for the sake of the big new scala-storage
      // patch this is fine.
      // val fileName = randomAlphanumericWithSpace()
      val fileName = i.toString
      val filePath = s"data/$subPath/$fileName.txt"
      val fileContents = Random.nextString(256)
      FileEntry(filePath, fileContents)
    }
  }

  def createValidDigest(string: String) =
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

case class FileRepr[T](t: T, entry: FileEntry)
case class FileEntry(name: String, contents: String)
