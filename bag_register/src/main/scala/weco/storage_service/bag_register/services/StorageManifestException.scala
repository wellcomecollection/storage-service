package weco.storage_service.bag_register.services

class StorageManifestException(message: String)
    extends RuntimeException(message)

class BadFetchLocationException(message: String)
    extends StorageManifestException(message)
