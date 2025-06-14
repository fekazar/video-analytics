package edu.misis.runner

enum class StreamState {
    IN_PROGRESS,
    AWAIT_TERMINATION,
    TERMINATED,
}

data class StreamEntity(
    val id: String, // Stream url
    val state: StreamState,
)
