package edu.misis.runner.repository

import java.time.Instant
import java.util.UUID

data class InferenceResultEntity(
    val timestamp: Instant,
    val streamId: UUID,
    val facesCount: Int,
)
