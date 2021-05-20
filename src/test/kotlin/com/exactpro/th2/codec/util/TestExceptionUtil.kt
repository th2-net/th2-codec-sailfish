package com.exactpro.th2.codec.util

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test


internal class TestExceptionUtil {

    @Test
    internal fun `check formatting exception messages`() {
        val ex = Exception("FirstLevel", Exception("SecondLevel", Exception("ThirdLevel")))
        assertEquals(
            listOf("FirstLevel", "Caused by: SecondLevel", "Caused by: ThirdLevel"),
            ex.getAllMessages()
        )
    }
}