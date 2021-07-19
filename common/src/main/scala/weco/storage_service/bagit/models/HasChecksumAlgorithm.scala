package weco.storage_service.bagit.models

import weco.storage_service.verify.HashingAlgorithm

trait HasChecksumAlgorithm {
  val checksumAlgorithm: HashingAlgorithm
}
