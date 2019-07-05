package uk.ac.wellcome.platform.archive.bagunpacker.fixtures

import java.io.{File, _}

import grizzled.slf4j.Logging
import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveOutputStream, ArchiveStreamFactory}
import org.apache.commons.compress.compressors.{CompressorOutputStream, CompressorStreamFactory}
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

trait CompressFixture[Namespace] extends StorageRandomThings with S3Fixtures with Logging {

  val defaultFileCount = 10

  def createObjectLocationWith(namespace: Namespace, path: String): ObjectLocation

  def withArchive[R](
    namespace: Namespace,
    archiveFile: File
  )(testWith: TestWith[ObjectLocation, R])(
    implicit streamStore: StreamStore[ObjectLocation, InputStreamWithLength]
  ): R = {
    val location = createObjectLocationWith(
      namespace = namespace,
      path = archiveFile.getName
    )

    streamStore.put(location)(
      new InputStreamWithLength(new FileInputStream(archiveFile), length = archiveFile.length())
    ) shouldBe a[Right[_, _]]


    testWith(location)
  }

  def createTgzArchiveWithRandomFiles(
    fileCount: Int = 10,
    maxDepth: Int = 4,
    minSize: Int = 265,
    maxSize: Int = 1024): (File, Seq[File], Set[ArchiveEntry]) =
    createTgzArchiveWithFiles(
      randomFilesInDirs(
        fileCount = fileCount,
        dirs = fileCount / 4,
        maxDepth = maxDepth,
        minSize = minSize,
        maxSize = maxSize
      )
    )

  def createTgzArchiveWithFiles(
    files: Seq[File]): (File, Seq[File], Set[ArchiveEntry]) =
    createArchiveWith(
      archiverName = "tar",
      compressorName = "gz",
      files = files
    )

  def createArchiveWithRandomFiles(
    archiverName: String,
    compressorName: String,
    fileCount: Int = 10
  ): (File, Seq[File], Set[ArchiveEntry]) =
    createArchiveWith(
      archiverName,
      compressorName,
      randomFilesInDirs(
        fileCount,
        fileCount / 4
      )
    )

  def createArchiveWith(
    archiverName: String,
    compressorName: String,
    files: Seq[File]
  ): (File, Seq[File], Set[ArchiveEntry]) = {
    val archiveFile = File.createTempFile(
      randomUUID.toString,
      ".test"
    )

    val fileOutputStream =
      new FileOutputStream(archiveFile)

    val archive = new Archive(
      archiverName,
      compressorName,
      fileOutputStream
    )

    val entries = files.map { file =>
      val entryName = relativeToTmpDir(file)
      debug(s"Archiving ${file.getAbsolutePath} in $entryName")
      archive.addFile(
        file,
        entryName
      )
    } toSet

    archive.finish()
    fileOutputStream.close()

    (archiveFile, files, entries)
  }

  def relativeToTmpDir(file: File): String =
    new File(tmpDir).toURI
      .relativize(file.toURI)
      .getPath

  private[fixtures] class Archive(
    archiverName: String,
    compressorName: String,
    outputStream: OutputStream
  ) {

    private val compress = compressor(compressorName)(_)
    private val pack = packer(archiverName)(_)

    private val compressorOutputStream =
      compress(outputStream)

    private val archiveOutputStream = pack(
      compressorOutputStream
    )

    def finish() = {
      archiveOutputStream.flush()
      archiveOutputStream.finish()
      compressorOutputStream.flush()
      compressorOutputStream.close()
    }

    def addFile(
      file: File,
      entryName: String
    ) = {
      synchronized {

        val entry = archiveOutputStream
          .createArchiveEntry(file, entryName)

        val fileInputStream = new BufferedInputStream(
          new FileInputStream(file)
        )

        archiveOutputStream.putArchiveEntry(entry)

        IOUtils.copy(
          fileInputStream,
          archiveOutputStream
        )

        archiveOutputStream.closeArchiveEntry()

        fileInputStream.close()

        entry
      }
    }

    private def compressor(
      compressorName: String
    )(
      outputStream: OutputStream
    ): CompressorOutputStream = {

      val compressorStreamFactory =
        new CompressorStreamFactory()

      val bufferedOutputStream =
        new BufferedOutputStream(outputStream)

      compressorStreamFactory
        .createCompressorOutputStream(
          compressorName,
          bufferedOutputStream
        )
    }

    private def packer(
      archiverName: String
    )(
      outputStream: OutputStream
    ): ArchiveOutputStream = {

      val archiveStreamFactory =
        new ArchiveStreamFactory()

      val bufferedOutputStream =
        new BufferedOutputStream(outputStream)

      archiveStreamFactory
        .createArchiveOutputStream(
          archiverName,
          bufferedOutputStream
        )

    }
  }
}

case class TestArchive(
  archiveFile: File,
  containedFiles: List[File],
  archiveEntries: Set[ArchiveEntry],
  location: ObjectLocation
)
