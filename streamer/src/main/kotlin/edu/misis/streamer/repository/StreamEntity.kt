package edu.misis.streamer.repository

import edu.misis.streamer.StreamState
import java.net.URI
import java.time.Instant
import java.util.*

data class StreamEntity(
    val id: UUID,
    val state: StreamState,
    val streamUrl: URI,
    val chunksBucket: String?,
    val updatedAt: Instant,
)