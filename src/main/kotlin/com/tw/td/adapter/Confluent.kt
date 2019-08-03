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
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.web.client.RestTemplate

class Confluent {
    val logger = Logger.getLogger(Confluent::class.java)

    companion object {
        private val restTemplate = RestTemplate()
    }

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

    private fun createPublisherStream(namespace: String): Any {
        val headers = HttpHeaders()
        headers.contentType = MediaType.APPLICATION_JSON

        val streamQuery = "CREATE STREAM " + namespace + "_stream" + "(eventType VARCHAR, data VARCHAR) WITH (KAFKA_TOPIC='" + namespace + "', VALUE_FORMAT='JSON');"
        val response: Response = khttp.post(
                url = "http://localhost:8088/ksql",
                json = mapOf("ksql" to streamQuery))
        val obj: JSONObject = response.jsonObject
        logger.debug(obj)
        return "some"
    }

    fun createPublisherTopic(namespace: Namespace): String {
        val newTopic = NewTopic(namespace.id, 1, 1.toShort())
        val collections = ArrayList<NewTopic>()
        collections.add(newTopic)
        createAdminClient().createTopics(collections)
        logger.debug(createPublisherStream(namespace.id!!))
        return "topic created"
    }


    fun publishEvent(payload: String, namespace: String, event: String): String {
        val jsonFormattedPayload = JSONObject(payload)
        logger.debug(String.format("#### -> Publishing to topic -> %s", (namespace + "." + event)))
        val jsonPayloadToTopic = JSONObject()
        jsonPayloadToTopic.put("eventType", namespace + "/" + event)
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

}