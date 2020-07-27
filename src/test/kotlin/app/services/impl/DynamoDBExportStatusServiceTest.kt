package app.services.impl

import app.services.ExportStatusService
import com.amazonaws.SdkClientException
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.GetItemResult
import com.amazonaws.services.dynamodbv2.model.QueryResult
import com.amazonaws.services.dynamodbv2.model.QueryRequest
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult
import com.amazonaws.services.dynamodbv2.model.Select
import com.nhaarman.mockitokotlin2.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.retry.annotation.EnableRetry
import org.springframework.test.context.junit4.SpringRunner
import org.junit.Assert.assertEquals

@RunWith(SpringRunner::class)
@EnableRetry
@SpringBootTest(classes = [DynamoDBExportStatusService::class])
class DynamoDBExportStatusServiceTest {

    @SpyBean
    @Autowired
    private lateinit var exportStatusService: ExportStatusService

    @MockBean
    private lateinit var amazonDynamoDB: AmazonDynamoDB

    @Before
    fun before() {
        System.setProperty("correlation_id", "123")
        System.setProperty("topic_name", "topic")
        reset(amazonDynamoDB)
    }

    @Test
    fun incrementSentCountRetries() {
        given(amazonDynamoDB.updateItem(any()))
                .willThrow(SdkClientException(""))
                .willThrow(SdkClientException(""))
                .willReturn(mock<UpdateItemResult>())
        exportStatusService.incrementSentCount("")
        verify(exportStatusService, times(3)).incrementSentCount("")
    }

    @Test
    fun setSentStatusRetries() {
        given(amazonDynamoDB.getItem(any()))
                .willThrow(SdkClientException(""))
                .willThrow(SdkClientException(""))
                .willReturn(mock<GetItemResult>())
        exportStatusService.setSentStatus()
        verify(exportStatusService, times(3)).setSentStatus()
    }

    @Test
    fun collectionRunIsCompleteRetries() {
        given(amazonDynamoDB.query(any()))
                .willThrow(SdkClientException(""))
                .willThrow(SdkClientException(""))
                .willReturn(mock<QueryResult>())
        exportStatusService.collectionRunIsComplete()
        verify(exportStatusService, times(3)).collectionRunIsComplete()
    }

    @Test
    fun setSentStatusSetsStatusIfFinished() {

        val status = mock<AttributeValue> {
            on { s } doReturn "Exported"
        }

        val filesExported = mock<AttributeValue> {
            on { n } doReturn "10"
        }

        val filesSent = mock<AttributeValue> {
            on { n } doReturn "10"
        }
        val record =
                mapOf("CollectionStatus" to status,
                "FilesExported" to filesExported,
                "FilesSent" to filesSent)

        val getItemResult = mock<GetItemResult> {
            on { item } doReturn record
        }

        given(amazonDynamoDB.getItem(any())).willReturn(getItemResult)
        given(amazonDynamoDB.updateItem(any())).willReturn(mock<UpdateItemResult>())
        exportStatusService.setSentStatus()
        verify(amazonDynamoDB, times(1)).updateItem(any())
    }

    @Test
    fun setSentStatusDoesNotSetStatusIfNotExported() {

        val status = mock<AttributeValue> {
            on { s } doReturn "Exporting"
        }

        val filesExported = mock<AttributeValue> {
            on { n } doReturn "10"
        }

        val filesSent = mock<AttributeValue> {
            on { n } doReturn "10"
        }
        val record =
                mapOf("CollectionStatus" to status,
                        "FilesExported" to filesExported,
                        "FilesSent" to filesSent)

        val getItemResult = mock<GetItemResult> {
            on { item } doReturn record
        }

        given(amazonDynamoDB.getItem(any())).willReturn(getItemResult)
        given(amazonDynamoDB.updateItem(any())).willReturn(mock<UpdateItemResult>())
        exportStatusService.setSentStatus()
        verify(amazonDynamoDB, times(0)).updateItem(any())
    }

    @Test
    fun setSentStatusDoesNotSetStatusIfNotAllFilesSent() {

        val status = mock<AttributeValue> {
            on { s } doReturn "Exported"
        }

        val filesExported = mock<AttributeValue> {
            on { n } doReturn "11"
        }

        val filesSent = mock<AttributeValue> {
            on { n } doReturn "10"
        }
        val record =
                mapOf("CollectionStatus" to status,
                        "FilesExported" to filesExported,
                        "FilesSent" to filesSent)

        val getItemResult = mock<GetItemResult> {
            on { item } doReturn record
        }

        val updateItemResult = mock<UpdateItemResult>()

        given(amazonDynamoDB.getItem(any())).willReturn(getItemResult)
        given(amazonDynamoDB.updateItem(any())).willReturn(updateItemResult)
        exportStatusService.setSentStatus()
        verify(amazonDynamoDB, times(0)).updateItem(any())
    }

    @Test
    fun collectionRunIsCompleteReturnsTrueIfAllCollectionsAreSent() {
        val queryResultExporting = mock<QueryResult> {
            on { count } doReturn 0
        }

        val queryResultExported = mock<QueryResult> {
            on { count } doReturn 0
        }

        val correlationIdAttribute = AttributeValue().apply { s = "123" }
        val exportedAttribute = AttributeValue().apply { s = "Exported" }
        val exportingAttribute = AttributeValue().apply { s = "Exporting" }
        val filesExportedAttribute = AttributeValue().apply { n = "0" }

        val queryRequestExporting = 
            QueryRequest().apply {
                tableName = "UCExportToCrownStatus"
                keyConditionExpression = "CorrelationId = :CorrelationId AND CollectionStatus = :CollectionStatus"
                expressionAttributeValues = mapOf("CorrelationId" to correlationIdAttribute,
                    "CollectionStatus" to exportingAttribute)
                select = Select.COUNT.toString()
            }
    
        val queryRequestExported =  
            QueryRequest().apply {
                tableName = "UCExportToCrownStatus"
                keyConditionExpression = "CorrelationId = :CorrelationId AND CollectionStatus = :CollectionStatus AND FilesExported > :FilesExported"
                expressionAttributeValues = mapOf("CorrelationId" to correlationIdAttribute,
                    "CollectionStatus" to exportedAttribute,
                    "FilesExported" to filesExportedAttribute)
                select = Select.COUNT.toString()
            }

        given(amazonDynamoDB.query(queryRequestExporting)).willReturn(queryResultExporting)
        given(amazonDynamoDB.query(queryRequestExported)).willReturn(queryResultExported)
        
        val expected = true
        val actual = exportStatusService.collectionRunIsComplete()
        
        assertEquals(expected, actual)
    }

    @Test
    fun collectionRunIsCompleteReturnsFalseIfCollectionsStillExporting() {
        val queryResultExporting = mock<QueryResult> {
            on { count } doReturn 1
        }

        val queryResultExported = mock<QueryResult> {
            on { count } doReturn 0
        }

        val correlationIdAttribute = AttributeValue().apply { s = "123" }
        val exportedAttribute = AttributeValue().apply { s = "Exported" }
        val exportingAttribute = AttributeValue().apply { s = "Exporting" }
        val filesExportedAttribute = AttributeValue().apply { n = "0" }

        val queryRequestExporting = 
            QueryRequest().apply {
                tableName = "UCExportToCrownStatus"
                keyConditionExpression = "CorrelationId = :CorrelationId AND CollectionStatus = :CollectionStatus"
                expressionAttributeValues = mapOf("CorrelationId" to correlationIdAttribute,
                    "CollectionStatus" to exportingAttribute)
                select = Select.COUNT.toString()
            }
    
        val queryRequestExported =  
            QueryRequest().apply {
                tableName = "UCExportToCrownStatus"
                keyConditionExpression = "CorrelationId = :CorrelationId AND CollectionStatus = :CollectionStatus AND FilesExported > :FilesExported"
                expressionAttributeValues = mapOf("CorrelationId" to correlationIdAttribute,
                    "CollectionStatus" to exportedAttribute,
                    "FilesExported" to filesExportedAttribute)
                select = Select.COUNT.toString()
            }

        given(amazonDynamoDB.query(queryRequestExporting)).willReturn(queryResultExporting)
        given(amazonDynamoDB.query(queryRequestExported)).willReturn(queryResultExported)
        
        val expected = false
        val actual = exportStatusService.collectionRunIsComplete()
        
        assertEquals(expected, actual)
    }

    @Test
    fun collectionRunIsCompleteReturnsFalseIfCollectionsStillSending() {
        val queryResultExporting = mock<QueryResult> {
            on { count } doReturn 0
        }

        val queryResultExported = mock<QueryResult> {
            on { count } doReturn 1
        }

        val correlationIdAttribute = AttributeValue().apply { s = "123" }
        val exportedAttribute = AttributeValue().apply { s = "Exported" }
        val exportingAttribute = AttributeValue().apply { s = "Exporting" }
        val filesExportedAttribute = AttributeValue().apply { n = "0" }

        val queryRequestExporting = 
            QueryRequest().apply {
                tableName = "UCExportToCrownStatus"
                keyConditionExpression = "CorrelationId = :CorrelationId AND CollectionStatus = :CollectionStatus"
                expressionAttributeValues = mapOf("CorrelationId" to correlationIdAttribute,
                    "CollectionStatus" to exportingAttribute)
                select = Select.COUNT.toString()
            }
    
        val queryRequestExported =  
            QueryRequest().apply {
                tableName = "UCExportToCrownStatus"
                keyConditionExpression = "CorrelationId = :CorrelationId AND CollectionStatus = :CollectionStatus AND FilesExported > :FilesExported"
                expressionAttributeValues = mapOf("CorrelationId" to correlationIdAttribute,
                    "CollectionStatus" to exportedAttribute,
                    "FilesExported" to filesExportedAttribute)
                select = Select.COUNT.toString()
            }

        given(amazonDynamoDB.query(queryRequestExporting)).willReturn(queryResultExporting)
        given(amazonDynamoDB.query(queryRequestExported)).willReturn(queryResultExported)
        
        val expected = false
        val actual = exportStatusService.collectionRunIsComplete()
        
        assertEquals(expected, actual)
    }

    @Test
    fun collectionRunIsCompleteReturnsFalseIfExportingCollectionsNotRetrieved() {
        val queryResultExported = mock<QueryResult> {
            on { count } doReturn 1
        }

        val correlationIdAttribute = AttributeValue().apply { s = "123" }
        val exportedAttribute = AttributeValue().apply { s = "Exported" }
        val exportingAttribute = AttributeValue().apply { s = "Exporting" }
        val filesExportedAttribute = AttributeValue().apply { n = "0" }

        val queryRequestExporting = 
            QueryRequest().apply {
                tableName = "UCExportToCrownStatus"
                keyConditionExpression = "CorrelationId = :CorrelationId AND CollectionStatus = :CollectionStatus"
                expressionAttributeValues = mapOf("CorrelationId" to correlationIdAttribute,
                    "CollectionStatus" to exportingAttribute)
                select = Select.COUNT.toString()
            }
    
        val queryRequestExported =  
            QueryRequest().apply {
                tableName = "UCExportToCrownStatus"
                keyConditionExpression = "CorrelationId = :CorrelationId AND CollectionStatus = :CollectionStatus AND FilesExported > :FilesExported"
                expressionAttributeValues = mapOf("CorrelationId" to correlationIdAttribute,
                    "CollectionStatus" to exportedAttribute,
                    "FilesExported" to filesExportedAttribute)
                select = Select.COUNT.toString()
            }

        given(amazonDynamoDB.query(queryRequestExporting))
            .willThrow(SdkClientException(""))
            .willThrow(SdkClientException(""))
            .willReturn(mock<QueryResult>())
        given(amazonDynamoDB.query(queryRequestExported)).willReturn(queryResultExported)
        
        val expected = false
        val actual = exportStatusService.collectionRunIsComplete()
        
        assertEquals(expected, actual)
    }

    @Test
    fun collectionRunIsCompleteReturnsFalseIfExportedCollectionsNotRetrieved() {
        val queryResultExporting = mock<QueryResult> {
            on { count } doReturn 1
        }

        val correlationIdAttribute = AttributeValue().apply { s = "123" }
        val exportedAttribute = AttributeValue().apply { s = "Exported" }
        val exportingAttribute = AttributeValue().apply { s = "Exporting" }
        val filesExportedAttribute = AttributeValue().apply { n = "0" }

        val queryRequestExporting = 
            QueryRequest().apply {
                tableName = "UCExportToCrownStatus"
                keyConditionExpression = "CorrelationId = :CorrelationId AND CollectionStatus = :CollectionStatus"
                expressionAttributeValues = mapOf("CorrelationId" to correlationIdAttribute,
                    "CollectionStatus" to exportingAttribute)
                select = Select.COUNT.toString()
            }
    
        val queryRequestExported =  
            QueryRequest().apply {
                tableName = "UCExportToCrownStatus"
                keyConditionExpression = "CorrelationId = :CorrelationId AND CollectionStatus = :CollectionStatus AND FilesExported > :FilesExported"
                expressionAttributeValues = mapOf("CorrelationId" to correlationIdAttribute,
                    "CollectionStatus" to exportedAttribute,
                    "FilesExported" to filesExportedAttribute)
                select = Select.COUNT.toString()
            }

        given(amazonDynamoDB.query(queryRequestExporting)).willReturn(queryResultExporting)
        given(amazonDynamoDB.query(queryRequestExported))
            .willThrow(SdkClientException(""))
            .willThrow(SdkClientException(""))
            .willReturn(mock<QueryResult>())
        
        val expected = false
        val actual = exportStatusService.collectionRunIsComplete()
        
        assertEquals(expected, actual)
    }
}
