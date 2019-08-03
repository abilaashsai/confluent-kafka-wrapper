package com.tw.td.adapter

import org.apache.kafka.clients.admin.AdminClient
import org.jboss.logging.Logger
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*
import org.json.JSONObject

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

    fun createTopic(payload: String, namespace: String, event: String): String {
        val jsonFormattedPayload = JSONObject(payload)
        logger.debug(String.format("#### -> Publishing to topic -> %s", (namespace + "." + event)))
        val jsonPayloadToTopic = JSONObject()
        jsonPayloadToTopic.put("eventType", namespace + "/" + event)
        jsonPayloadToTopic.put("data", jsonFormattedPayload.getJSONObject("data"))
        createProducer().send(ProducerRecord(namespace, jsonPayloadToTopic.toString()))
        return "success"
    }

    fun listTopics(): List<String>? {
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