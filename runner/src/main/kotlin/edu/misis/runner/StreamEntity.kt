package edu.misis.runner

import java.util.*

enum class DBStreamState {
    INIT_BUCKET,
    IN_PROGRESS,
    AWAIT_TERMINATION,
    TERMINATED,
}

data class StreamEntity(
    val id: UUID,
    val state: DBStreamState,
    val streamUrl: String,
    val chunksBucket: String?,
)
