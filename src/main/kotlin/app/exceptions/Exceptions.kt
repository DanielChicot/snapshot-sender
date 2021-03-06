package app.exceptions

class DataKeyDecryptionException(message: String) : Exception(message)

class DataKeyServiceUnavailableException(message: String) : Exception(message)

class WriterException(message: String) : Exception(message)

class SuccessException(message: String) : Exception(message)

class MetadataException(message: String) : Exception(message)

class BlockedTopicException(message: String): Exception(message)
