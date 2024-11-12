package no.nav.helse.speider

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.MultiGauge
import io.micrometer.core.instrument.MultiGauge.Row
import io.micrometer.core.instrument.Tags
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.prometheus.metrics.model.registry.PrometheusRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.time.delay
import no.nav.helse.rapids_rivers.*
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit.SECONDS
import java.util.*

private val logger = LoggerFactory.getLogger("no.nav.helse.speider.App")
val ignoredApps = emptySet<String>()

// tid mellom *nå* og forrige pong-melding for at en podd skal antas å være nede/ha lagg
private val terskelForAntattNede = Duration.ofSeconds(60)

fun main() {
    val env = System.getenv()

    val props = Properties().apply {
        put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, env.getValue("KAFKA_BROKERS"))
        put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name)
        put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "")
        put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "jks")
        put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12")
        put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, env.getValue("KAFKA_TRUSTSTORE_PATH"))
        put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,  env.getValue("KAFKA_CREDSTORE_PASSWORD"))
        put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, env.getValue("KAFKA_KEYSTORE_PATH"))
        put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, env.getValue("KAFKA_CREDSTORE_PASSWORD"))
    }
    val adminClient = AdminClient.create(props)
    val pingProducer = KafkaProducer(props, StringSerializer(), StringSerializer())

    val topic = env.getValue("KAFKA_RAPID_TOPIC")
    val partitionsCount = adminClient.describeTopics(listOf(topic))
        .allTopicNames()
        .get()
        .getValue(topic)
        .partitions()
        .size

    logger.info("$topic consist of $partitionsCount partitions")

    val appStates = AppStates()
    var scheduledPingJob: Job? = null
    var statusPrinterJob: Job? = null

    val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT, PrometheusRegistry.defaultRegistry, Clock.SYSTEM)
    RapidApplication.create(env, meterRegistry = meterRegistry).apply {
        register(object : RapidsConnection.StatusListener {
            override fun onStartup(rapidsConnection: RapidsConnection) {
                CoroutineScope(Dispatchers.IO).apply {
                    statusPrinterJob = launch { printerJob(meterRegistry, rapidsConnection, appStates) }
                    scheduledPingJob = launch { pinger(pingProducer, topic, partitionsCount) }
                }
            }

            override fun onShutdown(rapidsConnection: RapidsConnection) {
                scheduledPingJob?.cancel()
                statusPrinterJob?.cancel()
            }
        })

        River(this).apply {
            validate { it.demandValue("@event_name", "application_up") }
            validate { it.requireKey("app_name", "instance_id") }
            validate { it.require("@opprettet", JsonNode::asLocalDateTime) }
        }.register(object : River.PacketListener {
            override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
                appStates.up(packet["app_name"].asText(), packet["instance_id"].asText(), packet["@opprettet"].asLocalDateTime())
            }

            override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
                logger.error("forstod ikke application_up:\n${problems.toExtendedReport()}")
            }
        })

        River(this).apply {
            validate { it.demandValue("@event_name", "pong") }
            validate { it.requireKey("app_name", "instance_id") }
            validate { it.require("ping_time", JsonNode::asLocalDateTime) }
            validate { it.require("pong_time", JsonNode::asLocalDateTime) }
        }.register(object : River.PacketListener {
            override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
                val app = packet["app_name"].asText()
                val instance = packet["instance_id"].asText()
                val pingTime = packet["ping_time"].asLocalDateTime()
                val pongTime = packet["pong_time"].asLocalDateTime()

                logger.info("{}-{} svarte på ping etter {} sekunder (mottatt etter {} sekunder)", app, instance, SECONDS.between(pingTime, pongTime), SECONDS.between(pongTime, LocalDateTime.now()))
                appStates.ping(app, instance, pingTime, pongTime)
            }

            override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
                logger.error("forstod ikke pong:\n${problems.toExtendedReport()}")
            }
        })

        River(this).apply {
            validate { it.demandValue("@event_name", "application_down") }
            validate { it.requireKey("app_name", "instance_id") }
            validate { it.require("@opprettet", JsonNode::asLocalDateTime) }
        }.register(object : River.PacketListener {
            override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
                appStates.down(packet["app_name"].asText(), packet["instance_id"].asText(), packet["@opprettet"].asLocalDateTime())
            }

            override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
                logger.error("forstod ikke application_down:\n${problems.toExtendedReport()}")
            }
        })

        River(this).apply {
            validate { it.demandValue("@event_name", "application_stop") }
            validate { it.requireKey("app_name", "instance_id") }
            validate { it.require("@opprettet", JsonNode::asLocalDateTime) }
        }.register(object : River.PacketListener {
            override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
                appStates.down(packet["app_name"].asText(), packet["instance_id"].asText(), packet["@opprettet"].asLocalDateTime())
            }

            override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
                logger.error("forstod ikke application_stop:\n${problems.toExtendedReport()}")
            }
        })
    }.start()
}

private fun Boolean.toInt() = if (this) 1 else 0
private suspend fun CoroutineScope.printerJob(meterRegistry: MeterRegistry, rapidsConnection: RapidsConnection, appStates: AppStates) {
    while (isActive) {
        delay(Duration.ofSeconds(15))
        val threshold = LocalDateTime.now().minus(terskelForAntattNede)
        logger.info(appStates.reportString(threshold))

        MultiGauge.builder("app_status")
            .description("Gjeldende status på apps")
            .register(meterRegistry)
            .register(appStates.report(threshold).map { (app, state) ->
                Row.of(Tags.of("appnavn", app), state.toInt())
            })
        appStates.instances(threshold).also { report ->
            rapidsConnection.publish(JsonMessage.newMessage("app_status", mapOf(
                "threshold" to threshold,
                "states" to report.map { (appName, info) ->
                    mapOf<String, Any>(
                        "app" to appName,
                        "state" to info.first.toInt(),
                        "last_active_time" to info.second,
                        "instances" to info.third.map { (instanceId, lastActive, isUp) ->
                            mapOf(
                                "instance" to instanceId,
                                "last_active_time" to lastActive,
                                "state" to isUp.toInt()
                            )
                        }
                    )
                }
            )).toJson())
        }
    }
}

private suspend fun CoroutineScope.pinger(producer: Producer<String, String>, topic: String, partitionCount: Int) {
    while (isActive) {
        delay(Duration.ofSeconds(30))
        val packet = JsonMessage.newMessage("ping")
        packet["ping_time"] = LocalDateTime.now()
        // produces a ping to each partition
        repeat(partitionCount) { partition ->
            producer.send(ProducerRecord(topic, partition, UUID.randomUUID().toString(), packet.toJson()))
        }
        producer.flush()
    }
}
