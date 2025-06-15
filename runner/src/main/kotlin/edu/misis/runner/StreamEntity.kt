package edu.misis.runner

import java.util.*

data class StreamEntity(
    val id: UUID,
    val state: StreamState,
    val streamUrl: String,
    val chunksBucket: String?,
)
