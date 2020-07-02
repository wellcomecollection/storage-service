package uk.ac.wellcome.platform.archive.bag_register.services

class StorageManifestException(message: String)
    extends RuntimeException(message)

class BadFetchLocationException(message: String)
    extends StorageManifestException(message)
