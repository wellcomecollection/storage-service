package uk.ac.wellcome.platform.archive.common.fixtures

import java.io.{File, FileOutputStream}
import java.nio.file.Paths
import java.time.{Instant, LocalDate}
import java.util.UUID

import uk.ac.wellcome.messaging.fixtures.SQS.Queue
import uk.ac.wellcome.platform.archive.common.bagit.models._
import uk.ac.wellcome.platform.archive.common.ingests.models.{
  IngestID,
  StorageProvider
}
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.generators.RandomThings

import scala.util.Random

trait StorageRandomThings extends RandomThings {

  def randomChecksumValue = ChecksumValue(randomAlphanumeric)

  def randomInstant: Instant =
    Instant.now().plusSeconds(Random.nextInt().abs)

  private val collectionMax = 10

  val dummyQueue: Queue = Queue(
    url = "test://test-q",
    arn = "arn::sqs::test",
    visibilityTimeout = 1
  )

  def collectionOf[T](min: Int = 0, max: Int = collectionMax)(f: => T): Seq[T] =
    (1 to randomInt(from = min, to = max)).map { _ =>
      f
    }

  def chooseFrom[T](seq: Seq[T]): T =
    seq(Random.nextInt(seq.size))

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

  def randomFilesInDirs(
    fileCount: Int = 10,
    dirs: Int = 4,
    maxDepth: Int = 4,
    minSize: Int = 265,
    maxSize: Int = 1024
  ): Seq[File] = {

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
            s"${randomAlphanumericWithLength()}.test"
          )
          .toString
      )
    }
  }

  val tmpDir: String = System.getProperty("java.io.tmpdir")

  def randomFilesWithNames(
    fileNames: List[String],
    maxDepth: Int = 4,
    minSize: Int = 265,
    maxSize: Int = 1024
  ): Seq[File] = {
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

  def writeToOutputStream(
    path: String = s"${randomUUID.toString}.test"
  )(writeTo: FileOutputStream => Unit): File = {
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

  def randomLocalDate = {
    val startRange = -999999999
    val maxValue = 1999999998
    LocalDate.ofEpochDay(startRange + Random.nextInt(maxValue))
  }

  def createBagVersion: BagVersion =
    BagVersion(randomInt(from = 1, to = 100000))

  def randomHashingAlgorithm: HashingAlgorithm = {
    val algorithms = List(MD5, SHA1, SHA256, SHA512)

    algorithms(Random.nextInt(algorithms.length))
  }

  def createBagPath: BagPath = BagPath(randomAlphanumeric)

  def createBagPathWithPrefix(prefix: String, name: String): BagPath =
    BagPath(s"$prefix/$name")

  def createChecksumWith(algorithm: HashingAlgorithm = SHA256): Checksum =
    Checksum(algorithm = algorithm, value = randomChecksumValue)

  def createChecksum: Checksum =
    createChecksumWith()

  def createProvider: StorageProvider =
    StorageProvider(
      id = chooseFrom(StorageProvider.allowedValues)
    )
}
