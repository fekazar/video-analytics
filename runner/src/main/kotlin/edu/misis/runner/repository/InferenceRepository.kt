package edu.misis.runner.repository

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.Timestamp

@Repository
class InferenceRepository(
    private val jdbcTemplate: JdbcTemplate,
) {
    fun batchInsert(records: List<InferenceResultEntity>) {
        val sql = """
            insert into inference_events (instant, streamId, facesCount)
            values (?, ?, ?)
        """.trimIndent()
        jdbcTemplate.batchUpdate(
            sql,
            records,
            records.size,
        ) { ps, record ->
            ps.setTimestamp(1, Timestamp.from(record.timestamp))
            ps.setString(2, record.streamId.toString())
            ps.setInt(3, record.facesCount)
        }
    }
}