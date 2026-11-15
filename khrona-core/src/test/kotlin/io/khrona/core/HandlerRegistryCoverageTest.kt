package io.khrona.core

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class HandlerRegistryCoverageTest {
    @Test
    fun `register get hasHandler and remove cover registry lifecycle`() {
        val registry = HandlerRegistry()

        assertFalse(registry.hasHandler("job"))
        assertNull(registry.get("job"))
        assertEquals(emptySet<String>(), registry.registeredJobIds())

        registry.register("job") {}
        assertTrue(registry.hasHandler("job"))
        assertNotNull(registry.get("job"))
        assertEquals(setOf("job"), registry.registeredJobIds())

        registry.remove("job")
        assertFalse(registry.hasHandler("job"))
        assertNull(registry.get("job"))
        assertEquals(emptySet<String>(), registry.registeredJobIds())
    }
}
