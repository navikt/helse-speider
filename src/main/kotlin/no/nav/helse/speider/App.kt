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
            validate { it.demandValue("@event_name", "application_up") }
            validate { it.requireKey("app_name", "instance_id") }
            validate { it.require("@opprettet", JsonNode::asLocalDateTime) }
        }.register(object : River.PacketListener {
            override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
                appStates.up(packet["app_name"].asText(), packet["instance_id"].asText(), packet["@opprettet"].asLocalDateTime())
            }

            override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {
                logger.error("forstod ikke application_up:\n${problems.toExtendedReport()}")
            }
        })

        River(this).apply {
            validate { it.demandValue("@event_name", "pong") }
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

            override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {
                logger.error("forstod ikke pong:\n${problems.toExtendedReport()}")
            }
        })

        River(this).apply {
            validate { it.demandValue("@event_name", "application_down") }
            validate { it.requireKey("app_name", "instance_id") }
            validate { it.require("@opprettet", JsonNode::asLocalDateTime) }
        }.register(object : River.PacketListener {
            override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
                appStates.down(packet["app_name"].asText(), packet["instance_id"].asText(), packet["@opprettet"].asLocalDateTime())
            }

            override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {
                logger.error("forstod ikke application_down:\n${problems.toExtendedReport()}")
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
        }
        appStates.instances(threshold).also { report ->
            rapidsConnection.publish(JsonMessage.newMessage(mapOf(
                "@event_name" to "app_status",
                "@opprettet" to LocalDateTime.now(),
                "threshold" to threshold,
                "states" to report.map {
                    mapOf<String, Any>(
                        "app" to it.key,
                        "state" to it.value.first,
                        "last_active_time" to it.value.second
                    )
                }
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

    private val states = mutableListOf<App>()

    fun up(app: String, threshold: LocalDateTime) = App.up(states, app, threshold)
    fun up(app: String) = up(app, LocalDateTime.MIN)

    fun up(app: String, instance: String, time: LocalDateTime) {
        App.up(states, app, instance, time)
    }

    fun down(app: String, instance: String, time: LocalDateTime) {
        App.down(states, app, instance, time)
    }

    fun report(threshold: LocalDateTime): Map<String, Int> {
        return instances(threshold).mapValues { it.value.first }
    }

    fun instances(threshold: LocalDateTime): Map<String, Pair<Int, LocalDateTime>> {
        return App.instances(states, threshold)
    }

    fun reportString(threshold: LocalDateTime): String {
        return App.reportString(states, threshold)
    }

    private class App(
        private val name: String,
        private val instances: MutableList<Instance> = mutableListOf(),
        private var time: LocalDateTime
    ) {
        internal companion object {
            fun up(states: List<App>, app: String, threshold: LocalDateTime) =
                states.firstOrNull { it.name == app }?.let { Instance.up(it.instances, threshold) } ?: false

            fun up(states: MutableList<App>, app: String, instance: String, time: LocalDateTime) {
                Instance.up(findOrCreateApp(states, app, time).instances, instance, time)
            }

            fun down(states: MutableList<App>, app: String, instance: String, time: LocalDateTime) {
                Instance.down(findOrCreateApp(states, app, time).instances, instance, time)
            }

            fun instances(states: List<App>, threshold: LocalDateTime): Map<String, Pair<Int, LocalDateTime>> {
                return states.associateBy { it.name }
                    .mapValues { (if (Instance.up(it.value.instances, threshold)) 1 else 0) to it.value.time }
            }

            fun reportString(states: List<App>, threshold: LocalDateTime): String {
                val sb = StringBuffer()
                sb.append("Application states since ${threshold.format(timestampFormat)}:\n")
                states.forEach { app ->
                    sb.append("\t")
                        .append(app.name)
                        .append(": ")
                        .appendln(if (Instance.up(app.instances, threshold)) "UP" else "DOWN")
                    app.instances.forEach { sb.append("\t\t").appendln(it.toString()) }
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
