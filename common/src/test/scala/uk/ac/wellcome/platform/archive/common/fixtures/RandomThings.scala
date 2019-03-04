package uk.ac.wellcome.platform.archive.common.fixtures

import java.io.{File, FileOutputStream}
import java.nio.file.Paths
import java.time.LocalDate
import java.util.UUID

import uk.ac.wellcome.platform.archive.common.models.bagit._
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.util.Random

trait RandomThings {
  def randomAlphanumeric(length: Int = 8) = {
    Random.alphanumeric take length mkString
  }

  def randomPaths(maxDepth: Int = 4, dirs: Int = 4) = {
    (1 to dirs).map { _ =>
      val depth = Random.nextInt(maxDepth)

      (1 to depth).foldLeft("") { (memo, _) =>
        Paths.get(memo,randomAlphanumeric()).toString
      }
    }.toList
  }

  def randomAlphanumericWithSpace(length: Int = 8) = {
    val str = randomAlphanumeric(length).toCharArray

    // Randomly choose an index in the string
    // to replace with a space,
    // avoiding the beginning or the end.

    val spaceIndex = Random.nextInt(str.length - 2) + 1
    str.updated(spaceIndex, ' ')
  }

  def randomFilesInDirs(
                   fileCount: Int = 10,
                   dirs: Int = 4,
                   maxDepth: Int = 4,
                   minSize: Int = 265,
                   maxSize: Int = 1024) = {

    def createFile(name: String) = {
      val fileSize =
        Random.nextInt(maxSize - minSize) + minSize

      randomFile(fileSize, name)
    }


    val paths = randomPaths(maxDepth, dirs)

    (1 to fileCount).map { _ =>
      val index = Random.nextInt(paths.length)
      val path = paths(index)

      createFile(
        Paths.get(
          path,
          s"${randomAlphanumeric()}.test"
        ).toString
      )
    }.toList
  }

  val tmpDir = System.getProperty("java.io.tmpdir")

  def randomFile(
    size: Int = 256,
    path: String = s"${randomUUID.toString}.test"
  ) = {

    val absolutePath = Paths.get(tmpDir, path)

    println(absolutePath)

    val file = new File(absolutePath.toString)
    val parentDir = new File(absolutePath.getParent.toString)

    parentDir.mkdirs()

    val fileOutputStream = new FileOutputStream(file)
    val contents = randomAlphanumeric(size)

    fileOutputStream.write(contents.getBytes)
    fileOutputStream.close()

    file
  }

  def randomUUID = UUID.randomUUID()

  def randomSourceOrganisation =
    SourceOrganisation(randomAlphanumeric())

  def randomInternalSenderIdentifier =
    InternalSenderIdentifier(randomAlphanumeric())

  def randomInternalSenderDescription =
    InternalSenderDescription(randomAlphanumeric())

  def randomExternalDescription =
    ExternalDescription(randomAlphanumeric())

  def randomPayloadOxum =
    PayloadOxum(Random.nextLong().abs, Random.nextInt().abs)

  def randomLocalDate = {
    val startRange = -999999999
    val maxValue = 1999999998
    LocalDate.ofEpochDay(startRange + Random.nextInt(maxValue))
  }

  def randomBagPath = BagPath(randomAlphanumeric(15))

  def randomBucket = Bucket(randomAlphanumeric())
}
