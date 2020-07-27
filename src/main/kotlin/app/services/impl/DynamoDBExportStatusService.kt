package app.services.impl

import app.services.ExportStatusService
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.GetItemRequest
import com.amazonaws.services.dynamodbv2.model.QueryRequest
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest
import com.amazonaws.services.dynamodbv2.model.Select
import org.springframework.beans.factory.annotation.Value
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service
import uk.gov.dwp.dataworks.logging.DataworksLogger

@Service
class DynamoDBExportStatusService(private val dynamoDB: AmazonDynamoDB): ExportStatusService {

    @Retryable(value = [Exception::class],
            maxAttempts = maxAttempts,
            backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
    override fun incrementSentCount(fileSent: String) {
        val result = dynamoDB.updateItem(incrementFilesSentRequest())
        logger.info("Incremented files sent",
                "file_sent" to fileSent,
                "files_sent" to "${result.attributes["FilesSent"]?.n}")
    }

    @Retryable(value = [Exception::class],
            maxAttempts = maxAttempts,
            backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
    override fun setSentStatus(): Boolean =
            if (collectionIsComplete()) {
                val result = dynamoDB.updateItem(setStatusSentRequest())
                logger.info("Collection status after update",
                        "collection status" to "${result.attributes["CollectionStatus"]?.s}")
                true
            }
            else {
                false
            }

    @Retryable(value = [Exception::class],
            maxAttempts = maxAttempts,
            backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
    override fun collectionRunIsComplete(): Boolean = !collectionsStillExportingOrSending()

    private fun collectionIsComplete(): Boolean {
        val (currentStatus, filesExported, filesSent) = currentStatusAndCounts()
        val isComplete = currentStatus == "Exported" && filesExported == filesSent && filesExported > 0
        logger.info("Collection status", "current_status" to currentStatus,
                "files_exported" to "$filesExported",
                "files_sent" to "$filesSent",
                "is_complete" to "$isComplete")
        return isComplete
    }

    private fun collectionsStillExportingOrSending(): Boolean {
        val exportingCount = currentExportingCount()

        if (exportingCount < 0) {
            logger.warn("Could not check current exporting collections count",
                "exporting_count" to "$exportingCount")
            return true
        }
        else if (exportingCount > 0) {
            logger.info("Collections still exporting so full run has not finished",
                "exporting_count" to "$exportingCount")
            return true
        }

        logger.info("No collections currently still exporting",
            "exporting_count" to "$exportingCount")
        
        val exportedCount = currentExportedCount()

        if (exportedCount < 0) {
            logger.warn("Could not check count of exported collections of one or more snapshot files",
                "exported_count" to "$exportedCount")
            return true
        }
        else if (exportedCount > 0) {
            logger.info("Collections containing one or more snapshot files are currently still sending so full run has not finished",
                "exported_count" to "$exportedCount")
            return true
        }

        logger.info("No collections containing one or more snapshot files are currently still sending",
            "exporting_count" to "$exportedCount")

        return false
    }

    private fun currentExportingCount(): Int {
        val result = dynamoDB.query(getCountQueryRequestExporting())
        return result.count ?: -1
    }

    private fun currentExportedCount(): Int {
        val result = dynamoDB.query(getCountQueryRequestExported())
        return result.count ?: -1
    }

    private fun currentStatusAndCounts(): Triple<String, Int, Int> {
        val result = dynamoDB.getItem(getItemRequest())
        val item = result.item
        val status = item["CollectionStatus"]
        val filesExported = item["FilesExported"]
        val filesSent = item["FilesSent"]
        return Triple(status?.s ?: "", (filesExported?.n ?: "0").toInt(), (filesSent?.n ?: "0").toInt())
    }

    private fun incrementFilesSentRequest() =
            UpdateItemRequest().apply {
                tableName = statusTableName
                key = primaryKey
                updateExpression = "SET FilesSent = FilesSent + :x"
                expressionAttributeValues = mapOf(":x" to AttributeValue().apply { n = "1" })
                returnValues = "ALL_NEW"
            }


    private fun setStatusSentRequest() =
            UpdateItemRequest().apply {
                tableName = statusTableName
                key = primaryKey
                updateExpression = "SET CollectionStatus = :x"
                expressionAttributeValues = mapOf(":x" to AttributeValue().apply { s = "Sent" })
                returnValues = "ALL_NEW"
            }

    private fun getItemRequest() =
        GetItemRequest().apply {
            tableName = statusTableName
            key = primaryKey
            consistentRead = true
        }

    private fun getCountQueryRequestExporting() = 
        QueryRequest().apply {
            tableName = statusTableName
            keyConditionExpression = "CorrelationId = :CorrelationId AND CollectionStatus = :CollectionStatus"
            expressionAttributeValues = expressionValuesExporting
            select = Select.COUNT.toString()
        }

    private fun getCountQueryRequestExported() = 
        QueryRequest().apply {
            tableName = statusTableName
            keyConditionExpression = "CorrelationId = :CorrelationId AND CollectionStatus = :CollectionStatus AND FilesExported > :FilesExported"
            expressionAttributeValues = expressionValuesExported
            select = Select.COUNT.toString()
        }

    private val expressionValuesExporting by lazy {
        mapOf("CorrelationId" to stringAttribute(correlationId),
                "CollectionStatus" to stringAttribute("Exporting"))
    }

    private val expressionValuesExported by lazy {
        mapOf("CorrelationId" to stringAttribute(correlationId),
                "CollectionStatus" to stringAttribute("Exported"),
                "FilesExported" to numberAttribute("0"))
    }

    private val primaryKey by lazy {
        mapOf("CorrelationId" to stringAttribute(correlationId),
                "CollectionName" to stringAttribute(topicName))
    }

    private fun stringAttribute(value: String) = AttributeValue().apply { s = value }

    private fun numberAttribute(value: String) = AttributeValue().apply { n = value }

    @Value("\${dynamodb.status.table.name:UCExportToCrownStatus}")
    private lateinit var statusTableName: String

    private val correlationId by lazy { System.getProperty("correlation_id") }
    private val topicName by lazy { System.getProperty("topic_name") }

    companion object {
        val logger = DataworksLogger.getLogger(DynamoDBExportStatusService::class.toString())
        const val maxAttempts = 5
        const val initialBackoffMillis = 1000L
        const val backoffMultiplier = 2.0
    }
}
