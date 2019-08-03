package com.tw.td

import com.tw.td.adapter.Confluent
import com.tw.td.depricatedGatewayMiddleware.MigrateSubscription
import org.springframework.web.bind.annotation.*

@RestController
class Router {

    @PostMapping("/{namespace}/event/{event}")
    fun publishEvent(@RequestBody payload: String,
                     @PathVariable namespace: String,
                     @PathVariable event: String) = Confluent().createTopic(payload, namespace, event)

}
