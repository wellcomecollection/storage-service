package uk.ac.wellcome.platform.archive.common.storage.services

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import org.scalatest.{EitherValues, FunSpec, Matchers}
import org.scanamo.auto._
import org.scanamo.{Table => ScanamoTable}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.fixtures.StorageManifestVHSFixture
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.{StorageManifest, StorageSpace}
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.fixtures.{DynamoFixtures, S3Fixtures}
import uk.ac.wellcome.storage.store.HybridStoreEntry
import uk.ac.wellcome.storage.store.memory.MemoryVersionedStore
import uk.ac.wellcome.storage.{NoVersionExistsError, VersionAlreadyExistsError, WriteError}

trait BetterStorageManifestDaoTestCases[Context]
  extends FunSpec
    with Matchers
    with EitherValues
    with StorageManifestGenerators {
  def withContext[R](testWith: TestWith[Context, R]): R

  def withDao[R](testWith: TestWith[BetterStorageManifestDao, R])(implicit context: Context): R

  it("allows storing and retrieving a record") {
    val storageManifest = createStorageManifest

    val newStorageManifest = createStorageManifestWith(
      space = storageManifest.space,
      bagInfo = storageManifest.info,
      version = storageManifest.version
    )

    storageManifest.id shouldBe newStorageManifest.id

    withContext { implicit context =>
      withDao { dao =>
        // Empty get

        val getResultPreInsert = dao.getLatest(storageManifest.id)
        getResultPreInsert.left.value shouldBe a[NoVersionExistsError]

        // Insert

        val insertResult = dao.put(storageManifest)
        insertResult shouldBe a[Right[_, _]]

        val getResultPostInsert = dao.getLatest(storageManifest.id)
        getResultPostInsert.right.value shouldBe storageManifest

        // Update

        val updateResult = dao.put(newStorageManifest)
        updateResult.left.value shouldBe a[WriteError]
      }
    }
  }

  it("blocks putting two manifests with the same version") {
    val storageManifest = createStorageManifest

    withContext { implicit context =>
      withDao { dao =>
        dao.put(storageManifest).right.value shouldBe storageManifest
        dao.put(storageManifest).left.value shouldBe a[VersionAlreadyExistsError]
      }
    }
  }
}

class MemoryStorageManifestDaoTest
  extends BetterStorageManifestDaoTestCases[MemoryVersionedStore[BagId, HybridStoreEntry[StorageManifest, EmptyMetadata]]] {
  type MemoryStore = MemoryVersionedStore[BagId, HybridStoreEntry[StorageManifest, EmptyMetadata]]

  override def withContext[R](
    testWith: TestWith[MemoryStore, R]): R =
    testWith(
      MemoryVersionedStore[BagId, HybridStoreEntry[StorageManifest, EmptyMetadata]](initialEntries = Map.empty)
    )

  override def withDao[R](testWith: TestWith[BetterStorageManifestDao, R])(implicit store: MemoryStore): R =
    testWith(
      new MemoryStorageManifestDao(store)
    )
}

class DynamoStorageManifestDaoTest
  extends BetterStorageManifestDaoTestCases[(Table, Bucket)]
    with DynamoFixtures
    with S3Fixtures {
  override def withContext[R](testWith: TestWith[(Table, Bucket), R]): R =
    withLocalDynamoDbTable { table =>
      withLocalS3Bucket { bucket =>
        testWith((table, bucket))
      }
    }

  override def withDao[R](testWith: TestWith[BetterStorageManifestDao, R])(
    implicit context: (Table, Bucket)): R = {
    val (table, bucket) = context

    testWith(
      new DynamoStorageManifestDao(
        dynamoConfig = createDynamoConfigWith(table),
        s3Config = createS3ConfigWith(bucket)
      )
    )
  }

  it("encodes the bag ID as a string to use as a table key") {
    val storageManifest = createStorageManifestWith(
      space = StorageSpace("abc"),
      bagInfo = createBagInfoWith(
        externalIdentifier = ExternalIdentifier("123")
      )
    )

    withLocalDynamoDbTable { table =>
      withLocalS3Bucket { bucket =>
        implicit val context: (Table, Bucket) = (table, bucket)

        withDao { dao =>
          dao.put(storageManifest) shouldBe a[Right[_, _]]

          case class Identified(id: String)

          val storedIdentifiers = scanamo.exec(ScanamoTable[Identified](table.name).scan())
            .map { _.right.value }
            .map { _.id }

          storedIdentifiers shouldBe Seq("abc:123")
        }
      }
    }
  }

  override def createTable(table: Table): Table =
    createTableWithHashRangeKey(
      table,
      hashKeyName = "id",
      hashKeyType = ScalarAttributeType.S,
      rangeKeyName = "version",
      rangeKeyType = ScalarAttributeType.N
    )
}

class StorageManifestDaoTest
    extends FunSpec
    with Matchers
    with EitherValues
    with StorageManifestGenerators
    with StorageManifestVHSFixture {

  it("allows storing and retrieving a record") {
    val storageManifest = createStorageManifest

    val newStorageManifest = createStorageManifestWith(
      space = storageManifest.space,
      bagInfo = storageManifest.info,
      version = storageManifest.version
    )

    storageManifest.id shouldBe newStorageManifest.id

    implicit val index: StorageManifestIndex = createIndex
    implicit val typedStore: StorageManifestTypedStore = createTypedStore

    val dao: StorageManifestDao = createStorageManifestDao

    // Empty get

    val getResultPreInsert = dao.getLatest(storageManifest.id)
    getResultPreInsert.left.value shouldBe a[NoVersionExistsError]

    // Insert

    val insertResult = dao.put(storageManifest)
    insertResult shouldBe a[Right[_, _]]

    val getResultPostInsert = dao.getLatest(storageManifest.id)
    getResultPostInsert.right.value shouldBe storageManifest

    // Update

    val updateResult = dao.put(newStorageManifest)
    updateResult.left.value shouldBe a[WriteError]
  }

  it("stores a record under the appropriate ID and version") {
    implicit val index: StorageManifestIndex = createIndex

    val dao: StorageManifestDao = createStorageManifestDao

    val storageManifest = createStorageManifestWith(
      version = 2
    )

    val newStorageManifest = createStorageManifestWith(
      space = storageManifest.space,
      bagInfo = storageManifest.info,
      version = 3
    )

    dao.put(storageManifest).right.value shouldBe storageManifest
    dao.put(newStorageManifest).right.value shouldBe newStorageManifest

    index.entries.size shouldBe 2

    dao
      .get(id = storageManifest.id, version = storageManifest.version)
      .right
      .value shouldBe storageManifest
    dao
      .get(id = storageManifest.id, version = newStorageManifest.version)
      .right
      .value shouldBe newStorageManifest
  }

  it("blocks putting two manifests with the same version") {
    val dao: StorageManifestDao = createStorageManifestDao

    val storageManifest = createStorageManifest

    dao.put(storageManifest).right.value shouldBe storageManifest
    dao.put(storageManifest).left.value shouldBe a[VersionAlreadyExistsError]
  }
}
