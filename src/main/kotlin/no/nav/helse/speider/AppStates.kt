package no.nav.helse.speider

import java.time.Duration
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

internal class AppStates {
    private companion object {
        private val timestampFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    }

    private val lock = ReentrantReadWriteLock()
    private val states = mutableListOf<App>()

    fun up(app: String, instance: String, time: LocalDateTime) {
        lock.write {
            App.up(states, app, instance, time)
        }
    }

    fun ping(app: String, instance: String, pingTime: LocalDateTime, pongTime: LocalDateTime) {
        lock.write {
            App.ping(states, app, instance, pingTime, pongTime)
        }
    }

    fun down(app: String, instance: String, time: LocalDateTime) {
        lock.write {
            App.down(states, app, instance, time)
        }
    }

    fun report(threshold: LocalDateTime): Map<String, Boolean> {
        return lock.read {
            instances(threshold).mapValues { it.value.first }
        }
    }

    fun instances(threshold: LocalDateTime) = lock.read {
        App.instances(states.filterNot { it.isIgnored }, threshold)
    }

    fun reportString(threshold: LocalDateTime): String {
        return lock.read {
            App.reportString(states, threshold)
        }
    }

    private class App(
        private val name: String,
        private val instances: MutableList<Instance> = mutableListOf(),
        private var time: LocalDateTime
    ) {
        val isIgnored = name in ignoredApps

        private val downInstances: MutableList<Pair<String, LocalDateTime>> = mutableListOf()

        companion object {
            fun up(states: MutableList<App>, appName: String, instance: String, time: LocalDateTime) {
                val app = findOrCreateApp(states, appName, time)
                // re-active a downed app if we've received the application_up event
                app.downInstances.removeAll { it.first == instance }
                Instance.up(app.instances, instance, time)
            }

            fun ping(states: MutableList<App>, appName: String, instance: String, pingTime: LocalDateTime, pongTime: LocalDateTime) {
                val app = findOrCreateApp(states, appName, pongTime)
                if (app.downInstances.any { it.first == instance }) return // don't re-active a downed app
                app.downInstances.removeAll { it.second < LocalDateTime.now().minusHours(6) }
                Instance.up(app.instances, instance, pongTime, pingTime = pingTime)
            }

            fun down(states: MutableList<App>, appName: String, instance: String, time: LocalDateTime) {
                val app = findOrCreateApp(states, appName, time)
                if (!Instance.down(app.instances, instance, time)) return
                app.downInstances.add(instance to LocalDateTime.now())
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

            private fun findOrCreateApp(states: MutableList<App>, app: String, lastActivity: LocalDateTime): App {
                return states.findExistingAndUpdateActiveTime(app, lastActivity) ?: states.registerNewApp(app, lastActivity)
            }

            private fun List<App>.findExistingAndUpdateActiveTime(app: String, time: LocalDateTime): App? {
                return firstOrNull { it.name == app }?.also {
                    it.time = maxOf(it.time, time)
                }
            }

            private fun MutableList<App>.registerNewApp(app: String, time: LocalDateTime): App {
                return App(app, mutableListOf(), time).also { add(it) }
            }
        }
    }

    private class Instance(
        private val id: String,
        // the last time the app responded to a ping
        private var lastPingTime: LocalDateTime,
        // last time the app responded with an 'up' event or a 'pong'
        private var lastActivity: LocalDateTime,
        // last time speider registered the information
        private var lastUpdated: LocalDateTime
    ) {
        private val latency get() = Duration.between(lastPingTime, lastActivity)
        override fun toString(): String {
            val now = LocalDateTime.now()
            return "$id: last active ${Duration.between(lastActivity, now).toSeconds()} seconds ago (at ${lastActivity.format(timestampFormat)}) " +
                    "with ${latency.toSeconds()} seconds latency " +
                    "(as of ${Duration.between(lastUpdated, now).toSeconds()} seconds ago, on ${lastUpdated.format(timestampFormat)})"
        }

        fun up(threshold: LocalDateTime) = lastActivity >= threshold

        private fun updateLastActiveTime(pingTime: LocalDateTime?, newTime: LocalDateTime) {
            if (pingTime != null) this.lastPingTime = maxOf(lastPingTime, pingTime)
            this.lastActivity = maxOf(lastActivity, newTime)
            this.lastUpdated = LocalDateTime.now()
        }

        private fun isInstanceDown(instance: String, downTime: LocalDateTime): Boolean {
            if (this.id != instance) return false
            if (downTime < this.lastActivity) return false
            return true
        }

        companion object {
            fun list(list: List<Instance>, threshold: LocalDateTime) = list.map { Triple(it.id, it.lastActivity, it.up(threshold)) }
            fun up(list: MutableList<Instance>, instance: String, lastActivity: LocalDateTime, pingTime: LocalDateTime? = null) {
                 if (!list.updateLastActiveTime(instance, pingTime, lastActivity)) list.registerNewInstance(instance, lastActivity)
            }

            fun down(list: MutableList<Instance>, instance: String, time: LocalDateTime) =
                list.removeIf { it.isInstanceDown(instance, time) }

            fun up(list: MutableList<Instance>, threshold: LocalDateTime) =
                list.any { it.up(threshold) }

            private fun MutableList<Instance>.registerNewInstance(instance: String, time: LocalDateTime) {
                add(Instance(instance, time, time, LocalDateTime.now()))
            }

            private fun List<Instance>.updateLastActiveTime(instance: String, pingTime: LocalDateTime?, lastActivity: LocalDateTime): Boolean {
                val it = firstOrNull { it.id == instance } ?: return false
                it.updateLastActiveTime(pingTime, lastActivity)
                return true
            }
        }
    }
}