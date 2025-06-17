package edu.misis.runner

import java.net.URI
import java.util.*

data class StreamEntity(
    val id: UUID,
    val state: StreamState,
    val streamUrl: URI,
    val chunksBucket: String?,
)
