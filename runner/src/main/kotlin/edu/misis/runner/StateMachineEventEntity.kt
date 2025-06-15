package edu.misis.runner

import java.time.Instant
import java.util.*

// For simplicity only one instance should read this table
data class StateMachineEventEntity(
    val id: UUID,
    val createdAt: Instant,
    val type: String,
    val headers: Map<String, String> = emptyMap(),
    val lockedUntil: Instant? = null,
    val processedAt: Instant? = null,
)
