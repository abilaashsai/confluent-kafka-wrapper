package com.tw.td

import com.tw.td.adapter.Confluent
import org.springframework.web.bind.annotation.*

@RestController
class Router {

    val confluent = Confluent()

    @PostMapping("/{namespace}/event/{event}")
    fun publishEvent(@RequestBody payload: String,
                     @PathVariable namespace: String,
                     @PathVariable event: String) = confluent.createTopic(payload, namespace, event)

    @GetMapping("/listtopics")
    fun listTopics(): List<String>? = confluent.listTopics()

}
