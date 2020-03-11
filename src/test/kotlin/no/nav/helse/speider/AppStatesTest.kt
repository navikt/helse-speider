package no.nav.helse.speider

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDateTime

internal class AppStatesTest {

    private companion object {
        private const val APP = "my-app"
        private const val INSTANCE_1 = "instance1"
        private const val INSTANCE_2 = "instance2"
        private val now = LocalDateTime.now()
        private val OneMinuteAgo = now.minusMinutes(1)
        private val TwoMinutesAgo = now.minusMinutes(2)
        private val ThreeMinutesAgo = now.minusMinutes(3)
    }

    private lateinit var states: AppStates

    @BeforeEach
    fun setup() {
        states = AppStates()
    }

    @Test
    internal fun `app is up when any instance is up`() {
        states.up(APP, INSTANCE_1, now)
        assertTrue(states.up(APP))
        states.down(APP, INSTANCE_2, now)
        assertTrue(states.up(APP))
    }

    @Test
    internal fun `app is down when all instances are down`() {
        assertFalse(states.up(APP))
        states.down(APP, INSTANCE_1, now)
        assertFalse(states.up(APP))
        states.down(APP, INSTANCE_2, now)
        assertFalse(states.up(APP))
    }

    @Test
    internal fun `earlier events does not overwrite newer`() {
        states.up(APP, INSTANCE_1, now)
        assertTrue(states.up(APP))
        states.down(APP, INSTANCE_1, now.minusHours(1))
        assertTrue(states.up(APP))
    }

    @Test
    internal fun `newer events does overwrite earlier`() {
        states.up(APP, INSTANCE_1, now.minusHours(1))
        assertTrue(states.up(APP))
        states.down(APP, INSTANCE_1, now)
        assertFalse(states.up(APP))
    }

    @Test
    internal fun `assume inresponsive if last event is older than threshold`() {
        states.up(APP, INSTANCE_1, TwoMinutesAgo)
        assertTrue(states.up(APP, ThreeMinutesAgo))
        assertFalse(states.up(APP, OneMinuteAgo))
        states.up(APP, INSTANCE_1, now)
        assertTrue(states.up(APP, OneMinuteAgo))
    }

    @Test
    internal fun `up if any instance is above threshold`() {
        states.up(APP, INSTANCE_1, TwoMinutesAgo)
        states.down(APP, INSTANCE_2, OneMinuteAgo)
        assertTrue(states.up(APP, ThreeMinutesAgo))
        assertTrue(states.up(APP, TwoMinutesAgo))
        assertFalse(states.up(APP, OneMinuteAgo))
    }
}
