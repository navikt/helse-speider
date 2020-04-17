package no.nav.helse.speider

import com.fasterxml.jackson.databind.JsonNode
import io.prometheus.client.Gauge
import kotlinx.coroutines.*
import kotlinx.coroutines.time.delay
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.SECONDS

private val stateGauge = Gauge.build("app_status", "Gjeldende status på apps")
    .labelNames("appnavn")
    .register()
private val logger = LoggerFactory.getLogger("no.nav.helse.speider.App")

fun main() {
    val env = System.getenv()

    val appStates = AppStates()
    var scheduledPingJob: Job? = null
    var statusPrinterJob: Job? = null

    RapidApplication.create(env).apply {
        register(object : RapidsConnection.StatusListener {
            override fun onStartup(rapidsConnection: RapidsConnection) {
                statusPrinterJob = GlobalScope.launch { printerJob(rapidsConnection, appStates) }
                scheduledPingJob = GlobalScope.launch { pinger(rapidsConnection) }
            }

            override fun onShutdown(rapidsConnection: RapidsConnection) {
                scheduledPingJob?.cancel()
                statusPrinterJob?.cancel()
            }
        })

        River(this).apply {
            validate { it.requireValue("@event_name", "application_up") }
            validate { it.requireKey("app_name", "instance_id") }
            validate { it.require("@opprettet", JsonNode::asLocalDateTime) }
        }.register(object : River.PacketListener {
            override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
                appStates.up(packet["app_name"].asText(), packet["instance_id"].asText(), packet["@opprettet"].asLocalDateTime())
            }
        })

        River(this).apply {
            validate { it.requireValue("@event_name", "pong") }
            validate { it.requireKey("app_name", "instance_id") }
            validate { it.require("ping_time", JsonNode::asLocalDateTime) }
            validate { it.require("pong_time", JsonNode::asLocalDateTime) }
        }.register(object : River.PacketListener {
            override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
                val app = packet["app_name"].asText()
                val instance = packet["instance_id"].asText()
                val pingTime = packet["ping_time"].asLocalDateTime()
                val pongTime = packet["pong_time"].asLocalDateTime()

                logger.info("{}-{} svarte på ping etter {} sekunder", app, instance, SECONDS.between(pingTime, pongTime))
                appStates.up(app, instance, pongTime)
            }
        })

        River(this).apply {
            validate { it.requireValue("@event_name", "application_down") }
            validate { it.requireKey("app_name", "instance_id") }
            validate { it.require("@opprettet", JsonNode::asLocalDateTime) }
        }.register(object : River.PacketListener {
            override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
                appStates.down(packet["app_name"].asText(), packet["instance_id"].asText(), packet["@opprettet"].asLocalDateTime())
            }
        })
    }.start()
}

private suspend fun CoroutineScope.printerJob(rapidsConnection: RapidsConnection, appStates: AppStates) {
    while (isActive) {
        delay(Duration.ofSeconds(15))
        val threshold = LocalDateTime.now().minusMinutes(1)
        logger.info(appStates.reportString(threshold))
        appStates.report(threshold).also { report ->
            report.forEach { (app, state) ->
                stateGauge.labels(app).set(state.toDouble())
            }
            rapidsConnection.publish(JsonMessage.newMessage(mapOf(
                "@event_name" to "app_status",
                "@opprettet" to LocalDateTime.now(),
                "since" to threshold,
                "states" to report
            )).toJson())
        }
    }
}

private suspend fun CoroutineScope.pinger(rapidsConnection: RapidsConnection) {
    val packet = JsonMessage.newMessage(mapOf("@event_name" to "ping"))
    while (isActive) {
        delay(Duration.ofSeconds(30))
        packet["ping_time"] = LocalDateTime.now()
        rapidsConnection.publish(packet.toJson())
    }
}

internal class AppStates {
    private companion object {
        private val timestampFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    }

    private val states = mutableMapOf<String, MutableList<Instance>>()

    fun up(app: String, threshold: LocalDateTime) = states[app]?.let { Instance.up(it, threshold) } ?: false
    fun up(app: String) = up(app, LocalDateTime.MIN)

    fun up(app: String, instance: String, time: LocalDateTime) {
        Instance.up(states.getOrPut(app) { mutableListOf() }, instance, time)
    }

    fun down(app: String, instance: String, time: LocalDateTime) {
        Instance.down(states.getOrPut(app) { mutableListOf() }, instance, time)
    }

    fun report(threshold: LocalDateTime): Map<String, Int> {
        return states.mapValues { if (Instance.up(it.value, threshold)) 1 else 0 }
    }

    fun reportString(threshold: LocalDateTime): String {
        val sb = StringBuffer()
        sb.append("Application states since ${threshold.format(timestampFormat)}:\n")
        states.forEach { (app, instances) ->
            sb.append("\t")
                .append(app)
                .append(": ")
                .appendln(if (Instance.up(instances, threshold)) "UP" else "DOWN")
            instances.forEach { sb.append("\t\t").appendln(it.toString()) }
        }
        return sb.toString()
    }

    private class Instance(private val id: String, private var time: LocalDateTime) {
        override fun toString(): String {
            return "$id: last active at ${time.format(timestampFormat)}"
        }

        internal companion object {
            fun up(list: MutableList<Instance>, instance: String, time: LocalDateTime) {
                list.firstOrNull { it.id == instance }?.also {
                    if (it.time < time) it.time = time
                } ?: list.add(Instance(instance, time))
            }

            fun down(list: MutableList<Instance>, instance: String, time: LocalDateTime) {
                list.removeIf { it.id == instance && it.time < time }
            }

            fun up(list: MutableList<Instance>, threshold: LocalDateTime) =
                list.any { it.time >= threshold }
        }
    }
}
