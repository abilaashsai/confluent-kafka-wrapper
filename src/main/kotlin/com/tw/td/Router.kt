package com.tw.td

import com.tw.td.adapter.Confluent
import org.springframework.web.bind.annotation.*

@RestController
class Router {

    val confluent = Confluent()

    @PostMapping("/namespace")
    fun createNamespace(@RequestBody namespace: Namespace): String = confluent.createPublisherTopic(namespace)

    @PostMapping("/{namespace}/event/{event}")
    fun publishEvent(@RequestBody payload: String,
                     @PathVariable namespace: String,
                     @PathVariable event: String): String = confluent.publishEvent(payload, namespace, event)


    @PostMapping("/subscription")
    fun subscription(@RequestBody payload: String): String = confluent.subscribeEvent(payload)

    @GetMapping("/listtopics")
    fun listTopics(): List<String> = confluent.listTopics()

}
