package no.nav.helse.speider

import com.fasterxml.jackson.databind.JsonNode
import io.prometheus.client.Gauge
import kotlinx.coroutines.*
import kotlinx.coroutines.time.delay
import no.nav.helse.rapids_rivers.*
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.SECONDS
import java.util.*

private val stateGauge = Gauge.build("app_status", "Gjeldende status på apps")
    .labelNames("appnavn")
    .register()
private val logger = LoggerFactory.getLogger("no.nav.helse.speider.App")

fun main() {
    val env = System.getenv()

    val adminClient = AdminClient.create(Properties().apply {
        put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, env.getValue("KAFKA_BROKERS"))
        put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name)
        put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "")
        put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "jks")
        put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12")
        put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, env.getValue("KAFKA_TRUSTSTORE_PATH"))
        put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,  env.getValue("KAFKA_CREDSTORE_PASSWORD"))
        put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, env.getValue("KAFKA_KEYSTORE_PATH"))
        put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, env.getValue("KAFKA_CREDSTORE_PASSWORD"))
    })

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

    RapidApplication.create(env).apply {
        register(object : RapidsConnection.StatusListener {
            override fun onStartup(rapidsConnection: RapidsConnection) {
                statusPrinterJob = GlobalScope.launch { printerJob(rapidsConnection, appStates) }
                scheduledPingJob = GlobalScope.launch { pinger(rapidsConnection, partitionsCount) }
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
            override fun onPacket(packet: JsonMessage, context: MessageContext) {
                appStates.up(packet["app_name"].asText(), packet["instance_id"].asText(), packet["@opprettet"].asLocalDateTime())
            }

            override fun onError(problems: MessageProblems, context: MessageContext) {
                logger.error("forstod ikke application_up:\n${problems.toExtendedReport()}")
            }
        })

        River(this).apply {
            validate { it.demandValue("@event_name", "pong") }
            validate { it.requireKey("app_name", "instance_id") }
            validate { it.require("ping_time", JsonNode::asLocalDateTime) }
            validate { it.require("pong_time", JsonNode::asLocalDateTime) }
        }.register(object : River.PacketListener {
            override fun onPacket(packet: JsonMessage, context: MessageContext) {
                val app = packet["app_name"].asText()
                val instance = packet["instance_id"].asText()
                val pingTime = packet["ping_time"].asLocalDateTime()
                val pongTime = packet["pong_time"].asLocalDateTime()

                logger.info("{}-{} svarte på ping etter {} sekunder", app, instance, SECONDS.between(pingTime, pongTime))
                appStates.up(app, instance, pongTime)
            }

            override fun onError(problems: MessageProblems, context: MessageContext) {
                logger.error("forstod ikke pong:\n${problems.toExtendedReport()}")
            }
        })

        River(this).apply {
            validate { it.demandValue("@event_name", "application_down") }
            validate { it.requireKey("app_name", "instance_id") }
            validate { it.require("@opprettet", JsonNode::asLocalDateTime) }
        }.register(object : River.PacketListener {
            override fun onPacket(packet: JsonMessage, context: MessageContext) {
                appStates.down(packet["app_name"].asText(), packet["instance_id"].asText(), packet["@opprettet"].asLocalDateTime())
            }

            override fun onError(problems: MessageProblems, context: MessageContext) {
                logger.error("forstod ikke application_down:\n${problems.toExtendedReport()}")
            }
        })
    }.start()
}

private fun Boolean.toInt() = if (this) 1 else 0
private suspend fun CoroutineScope.printerJob(rapidsConnection: RapidsConnection, appStates: AppStates) {
    while (isActive) {
        delay(Duration.ofSeconds(15))
        val threshold = LocalDateTime.now().minusMinutes(1)
        logger.info(appStates.reportString(threshold))
        appStates.report(threshold).onEach { (app, state) ->
            stateGauge.labels(app).set(state.toInt().toDouble())
        }
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

private suspend fun CoroutineScope.pinger(rapidsConnection: RapidsConnection, partitionCount: Int) {
    while (isActive) {
        delay(Duration.ofSeconds(30))
        val packet = JsonMessage.newMessage("ping")
        packet["ping_time"] = LocalDateTime.now()
        // when publishing a record without a key (and partition),
        // the record will be produced to the partitions in a round-robin manner.
        // Ensure that a ping is sent to each partition by repeating it as many times as the number of partitions
        repeat(partitionCount) { rapidsConnection.publish(packet.toJson()) }
    }
}

internal class AppStates {
    private companion object {
        private val timestampFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    }

    private val states = mutableListOf<App>()

    fun up(app: String, threshold: LocalDateTime) = App.up(states, app, threshold)
    fun up(app: String) = up(app, LocalDateTime.MIN)

    fun up(app: String, instance: String, time: LocalDateTime) {
        App.up(states, app, instance, time)
    }

    fun down(app: String, instance: String, time: LocalDateTime) {
        App.down(states, app, instance, time)
    }

    fun report(threshold: LocalDateTime): Map<String, Boolean> {
        return instances(threshold).mapValues { it.value.first }
    }

    fun instances(threshold: LocalDateTime) = App.instances(states, threshold)

    fun reportString(threshold: LocalDateTime): String {
        return App.reportString(states, threshold)
    }

    private class App(
        private val name: String,
        private val instances: MutableList<Instance> = mutableListOf(),
        private var time: LocalDateTime
    ) {
        companion object {
            fun up(states: List<App>, app: String, threshold: LocalDateTime) =
                states.firstOrNull { it.name == app }?.let { Instance.up(it.instances, threshold) } ?: false

            fun up(states: MutableList<App>, app: String, instance: String, time: LocalDateTime) {
                Instance.up(findOrCreateApp(states, app, time).instances, instance, time)
            }

            fun down(states: MutableList<App>, app: String, instance: String, time: LocalDateTime) {
                Instance.down(findOrCreateApp(states, app, time).instances, instance, time)
            }

            fun instances(states: List<App>, threshold: LocalDateTime): Map<String, Triple<Boolean, LocalDateTime, List<Triple<String, LocalDateTime, Boolean>>>> {
                return states.associate { app ->
                    app.name to Triple(Instance.up(app.instances, threshold), app.time, Instance.list(app.instances, threshold))
                }
            }

            fun reportString(states: List<App>, threshold: LocalDateTime): String {
                val sb = StringBuffer()
                sb.append("Application states since ${threshold.format(timestampFormat)}:\n")
                states.forEach { app ->
                    sb.append("\t")
                        .append(app.name)
                        .append(": ")
                        .appendLine(if (Instance.up(app.instances, threshold)) "UP" else "DOWN")
                    app.instances.forEach { sb.append("\t\t").appendLine(it.toString()) }
                }
                return sb.toString()
            }

            private fun findOrCreateApp(states: MutableList<App>, app: String, time: LocalDateTime) =
                states.firstOrNull { it.name == app }?.also {
                    it.time = maxOf(it.time, time)
                } ?: App(app, mutableListOf(), time).also {
                    states.add(it)
                }
        }
    }

    private class Instance(private val id: String, private var time: LocalDateTime) {
        override fun toString(): String {
            return "$id: last active at ${time.format(timestampFormat)}"
        }

        fun up(threshold: LocalDateTime) = time >= threshold

        internal companion object {
            fun list(list: List<Instance>, threshold: LocalDateTime) = list.map { Triple(it.id, it.time, it.up(threshold)) }
            fun up(list: MutableList<Instance>, instance: String, time: LocalDateTime) {
                list.firstOrNull { it.id == instance }?.also {
                    if (it.time < time) it.time = time
                } ?: list.add(Instance(instance, time))
            }

            fun down(list: MutableList<Instance>, instance: String, time: LocalDateTime) {
                list.removeIf { it.id == instance && it.time < time }
            }

            fun up(list: MutableList<Instance>, threshold: LocalDateTime) =
                list.any { it.up(threshold) }
        }
    }
}
