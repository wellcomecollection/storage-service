package uk.ac.wellcome.platform.archive.common.fixtures

import java.io.{File, FileOutputStream}
import java.nio.file.Paths
import java.time.LocalDate
import java.util.UUID

import uk.ac.wellcome.platform.archive.common.bagit.models._
import uk.ac.wellcome.platform.archive.common.ingests.models.IngestID
import uk.ac.wellcome.storage.generators.RandomThings

import scala.util.Random

trait StorageRandomThings extends RandomThings {
  def randomAlphanumericWithLength(length: Int = 8) =
    Random.alphanumeric take length mkString

  def randomPaths(maxDepth: Int = 4, dirs: Int = 4): List[String] = {
    (1 to dirs).map { _ =>
      val depth = Random.nextInt(maxDepth)

      (1 to depth).foldLeft("") { (memo, _) =>
        Paths.get(memo, randomAlphanumeric).toString
      }
    }.toList
  }

  def randomAlphanumericWithSpace(length: Int = 8): Array[Char] = {
    val str = randomAlphanumericWithLength(length).toCharArray

    // Randomly choose an index in the string
    // to replace with a space,
    // avoiding the beginning or the end.

    val spaceIndex = Random.nextInt(str.length - 2) + 1
    str.updated(spaceIndex, ' ')
  }

  def randomFilesInDirs(fileCount: Int = 10,
                        dirs: Int = 4,
                        maxDepth: Int = 4,
                        minSize: Int = 265,
                        maxSize: Int = 1024): Seq[File] = {

    def createFile(name: String) = {
      val fileSize =
        Random.nextInt(maxSize - minSize) + minSize

      randomFile(
        size = fileSize,
        path = name,
        useBytes = true
      )
    }

    val paths = randomPaths(maxDepth, dirs)

    (1 to fileCount).map { _ =>
      val index = Random.nextInt(paths.length)
      val path = paths(index)

      createFile(
        Paths
          .get(
            path,
            s"${ randomAlphanumericWithLength()}.test"
          )
          .toString
      )
    }
  }

  val tmpDir: String = System.getProperty("java.io.tmpdir")

  def randomFilesWithNames(fileNames: List[String],
                           maxDepth: Int = 4,
                           minSize: Int = 265,
                           maxSize: Int = 1024): Seq[File] = {
    def createFile(name: String) = {
      val fileSize =
        Random.nextInt(maxSize - minSize) + minSize

      randomFile(
        size = fileSize,
        path = name,
        useBytes = true
      )
    }

    fileNames.map { fileName =>
      createFile(fileName)
    }
  }

  def writeToOutputStream(path: String = s"${randomUUID.toString}.test")(
    writeTo: FileOutputStream => Unit): File = {
    val absolutePath = Paths.get(tmpDir, path)

    val file = absolutePath.toFile
    val parentDir = absolutePath.getParent.toFile

    parentDir.mkdirs()

    val fileOutputStream = new FileOutputStream(file)
    writeTo(fileOutputStream)
    fileOutputStream.close()

    file
  }

  def randomFile(
    size: Int = 256,
    path: String = s"${randomUUID.toString}.test",
    useBytes: Boolean = false
  ): File =
    writeToOutputStream(path) { fileOutputStream =>
      val bytes = if (useBytes) {
        randomBytes(size)
      } else {
        randomAlphanumericWithLength(size).getBytes
      }

      fileOutputStream.write(bytes)
    }

  def randomUUID: UUID = UUID.randomUUID()

  def createIngestID: IngestID =
    IngestID(randomUUID)

  def randomSourceOrganisation =
    SourceOrganisation(randomAlphanumericWithLength())

  def randomInternalSenderIdentifier =
    InternalSenderIdentifier(randomAlphanumericWithLength())

  def randomInternalSenderDescription =
    InternalSenderDescription(randomAlphanumericWithLength())

  def randomExternalDescription =
    ExternalDescription(randomAlphanumericWithLength())

  def randomPayloadOxum =
    PayloadOxum(Random.nextLong().abs, Random.nextInt().abs)

  def randomLocalDate = {
    val startRange = -999999999
    val maxValue = 1999999998
    LocalDate.ofEpochDay(startRange + Random.nextInt(maxValue))
  }
}
