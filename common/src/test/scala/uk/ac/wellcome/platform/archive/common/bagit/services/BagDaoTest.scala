package uk.ac.wellcome.platform.archive.common.bagit.services

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.fixtures.BagLocationFixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures

class BagDaoTest extends FunSpec with Matchers with S3Fixtures with BagLocationFixtures with EitherValues {
  val bagDao = new BagDao()

  it("gets a correctly formed bag") {
    val bagInfo = createBagInfo

    withLocalS3Bucket { bucket =>
      withBag(bucket, bagInfo = bagInfo) { case (rootLocation, _) =>
        bagDao.get(rootLocation).right.value.info shouldBe bagInfo
      }
    }
  }

  it("errors if the bag-info.txt does not exist") {
    withLocalS3Bucket { bucket =>
      withBag(bucket) { case (rootLocation, _) =>
        s3Client.deleteObject(
          rootLocation.namespace,
          rootLocation.join("bag-info.txt").path
        )

        bagDao.get(rootLocation).left.value.msg should startWith("Error loading bag-info.txt")
      }
    }
  }

  it("errors if the bag-info.txt file is malformed") {
    withLocalS3Bucket { bucket =>
      withBag(bucket) { case (rootLocation, _) =>
        s3Client.putObject(
          rootLocation.namespace,
          rootLocation.join("bag-info.txt").path,
          randomAlphanumeric
        )

        bagDao.get(rootLocation).left.value.msg should startWith("Error loading bag-info.txt")
      }
    }
  }

  it("errors if the file manifest does not exist") {
    withLocalS3Bucket { bucket =>
      withBag(bucket) { case (rootLocation, _) =>
        s3Client.deleteObject(
          rootLocation.namespace,
          rootLocation.join("manifest-sha256.txt").path
        )

        bagDao.get(rootLocation).left.value.msg should startWith("Error loading manifest-sha256.txt")
      }
    }
  }

  it("errors if the file manifest is malformed") {
    withLocalS3Bucket { bucket =>
      withBag(bucket) { case (rootLocation, _) =>
        s3Client.putObject(
          rootLocation.namespace,
          rootLocation.join("manifest-sha256.txt").path,
          randomAlphanumeric
        )

        bagDao.get(rootLocation).left.value.msg should startWith("Error loading manifest-sha256.txt")
      }
    }
  }

  it("errors if the tag manifest does not exist") {
    withLocalS3Bucket { bucket =>
      withBag(bucket) { case (rootLocation, _) =>
        s3Client.deleteObject(
          rootLocation.namespace,
          rootLocation.join("tagmanifest-sha256.txt").path
        )

        bagDao.get(rootLocation).left.value.msg should startWith("Error loading tagmanifest-sha256.txt")
      }
    }
  }

  it("errors if the tag manifest is malformed") {
    withLocalS3Bucket { bucket =>
      withBag(bucket) { case (rootLocation, _) =>
        s3Client.putObject(
          rootLocation.namespace,
          rootLocation.join("tagmanifest-sha256.txt").path,
          randomAlphanumeric
        )

        bagDao.get(rootLocation).left.value.msg should startWith("Error loading tagmanifest-sha256.txt")
      }
    }
  }

  it("passes if the fetch.txt does not exist") {
    val bagInfo = createBagInfo

    withLocalS3Bucket { bucket =>
      withBag(bucket, bagInfo = bagInfo) { case (rootLocation, _) =>
        s3Client.deleteObject(
          rootLocation.namespace,
          rootLocation.join("fetch.txt").path
        )

        bagDao.get(rootLocation).right.value.fetch shouldBe None
      }
    }
  }

  it("errors if the fetch file is malformed") {
    withLocalS3Bucket { bucket =>
      withBag(bucket) { case (rootLocation, _) =>
        s3Client.putObject(
          rootLocation.namespace,
          rootLocation.join("fetch.txt").path,
          randomAlphanumeric
        )

        bagDao.get(rootLocation).left.value.msg should startWith("Error loading fetch.txt")
      }
    }
  }
}
