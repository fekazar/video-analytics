package edu.misis.streamer.repository

import java.time.Instant

data class IntervalAverageProj(
    val start: Instant,
    val end: Instant,
    val avgFacesCount: Double,
    val samplesCount: Int,
)
