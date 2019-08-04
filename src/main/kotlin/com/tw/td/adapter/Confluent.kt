package com.tw.td.adapter

import com.tw.td.Namespace
import khttp.responses.Response
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.jboss.logging.Logger
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*
import org.json.JSONObject
import kotlin.collections.ArrayList

class Confluent {
    val logger = Logger.getLogger(Confluent::class.java)

    private fun getProperties(): Properties {
        val props = Properties()
        props["bootstrap.servers"] = "localhost:9092"
        props["key.serializer"] = StringSerializer::class.java
        props["value.serializer"] = StringSerializer::class.java
        return props
    }

    private fun createProducer(): KafkaProducer<String, String> {
        return KafkaProducer(getProperties())
    }

    private fun createAdminClient(): AdminClient {
        return AdminClient.create(getProperties())
    }

    private fun ksqlRequest(streamQuery: String): Any {
        Thread.sleep(8000)

        val response: Response = khttp.post(
                url = "http://localhost:8088/ksql",
                json = mapOf("ksql" to streamQuery))
        return response
    }

    private fun createPublisherStream(namespace: String): Any {
        val streamQuery = """CREATE STREAM stream_$namespace
            (eventType VARCHAR, data VARCHAR)
            WITH (KAFKA_TOPIC='$namespace', VALUE_FORMAT='JSON');"""
        return ksqlRequest(streamQuery)
    }

    private fun createConnector(url: String, subscriptionName: String): Any {
        val subscriptionName = subscriptionName.toUpperCase()
        val response: Response = khttp.post(
                url = "http://localhost:8083/connectors",
                json = mapOf("name" to subscriptionName,
                        "config" to mapOf("connector.class" to "io.confluent.connect.http.HttpSinkConnector",
                                "tasks.max" to "1",
                                "http.api.url" to url,
                                "topics" to "SUB_$subscriptionName",
                                "headers" to "Content-Type:application/vnd.kafka.json.v2+json|Accept:application/vnd.kafka.v2+json",
                                "value.converter" to "org.apache.kafka.connect.storage.StringConverter",
                                "confluent.topic.bootstrap.servers" to "localhost:9092",
                                "confluent.topic.replication.factor" to "1"
                        )),
                headers = mapOf("Content-Type" to "application/json"))

        logger.debug(response)
        return response

    }

    private fun createConsumerStream(namespace: String,
                                     subscription: String,
                                     filterStatement: String): Any {
        val streamQuery = """CREATE STREAM sub_$subscription
            AS SELECT data
            FROM stream_$namespace
            WHERE $filterStatement;"""
        return ksqlRequest(streamQuery)
    }

    fun createPublisherTopic(namespace: Namespace): String {
        val newTopic = NewTopic(namespace.id, 1, 1.toShort())

        val collections = ArrayList<NewTopic>()
        collections.add(newTopic)
        createAdminClient().createTopics(collections)

        createPublisherStream(namespace.id!!)
        logger.debug("stream created")
        return "topic created"
    }


    fun publishEvent(payload: String,
                     namespace: String, event: String): String {
        val jsonFormattedPayload = JSONObject(payload)
        logger.debug(String.format("#### -> Publishing to topic -> %s", namespace))

        val jsonPayloadToTopic = JSONObject()
        jsonPayloadToTopic.put("eventType", event)
        jsonPayloadToTopic.put("data", jsonFormattedPayload.getJSONObject("data"))

        createProducer().send(ProducerRecord(namespace, jsonPayloadToTopic.toString()))
        return "success"
    }

    fun listTopics(): List<String> {
        val adminClient = createAdminClient()
        val data = adminClient.listTopics()
        val availableTopics = data.namesToListings().get()
        val topics = (availableTopics.entries.filter { x ->
            !x.key.startsWith("_") &&
                    !x.key.startsWith("connect") &&
                    !x.key.startsWith("default")
        }.map { y -> y.key })

        return topics
    }


    fun subscribeEvent(payload: String): String {
        val jsonFormattedPayload = JSONObject(payload)
        val subscriptionName = jsonFormattedPayload.getString("name")
        val namespaceName = jsonFormattedPayload.getString("namespace")
        val subscriptionURL = jsonFormattedPayload.getJSONObject("notification")
                .getString("url")

        val newTopic = NewTopic("sub_$subscriptionName", 1, 1.toShort())
        val collections = ArrayList<NewTopic>()
        collections.add(newTopic)
        createAdminClient().createTopics(collections)

        val filterStatement = jsonFormattedPayload.getJSONArray("filter")
                .map { filter -> "eventType='$filter'" }.joinToString(separator = " OR ")
        createConsumerStream(namespaceName, subscriptionName, filterStatement)

        createConnector(subscriptionURL, subscriptionName)

        return "success"
    }

}