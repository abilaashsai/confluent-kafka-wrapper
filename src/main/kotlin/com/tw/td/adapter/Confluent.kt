package com.tw.td.adapter

import org.jboss.logging.Logger
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*
import org.json.JSONObject

class Confluent {
    val logger = Logger.getLogger(Confluent::class.java)

    private fun createProducer(): KafkaProducer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = "localhost:9092"
        props["key.serializer"] = StringSerializer::class.java
        props["value.serializer"] = StringSerializer::class.java
        return KafkaProducer(props)
    }

    fun createTopic(payload: String, namespace: String, event: String): String {
        val jsonFormattedPayload = JSONObject(payload)
        logger.debug(String.format("#### -> Publishing to topic -> %s", (namespace + "." + event)))
        val jsonData = jsonFormattedPayload.getJSONObject("data")
        val topicName = namespace + "." + event
        createProducer().send(ProducerRecord(topicName, jsonData.toString()))
        return "success"
    }
}