package edu.misis.runner

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import org.springframework.stereotype.Repository
import java.net.URI
import java.sql.ResultSet

private class StreamRowMapper : RowMapper<StreamEntity> {
    override fun mapRow(rs: ResultSet, rowNum: Int): StreamEntity {
        return StreamEntity(
            rs.getString("id"),
            StreamState.valueOf(rs.getString("state")),
        )
    }
}

@Repository
class StreamRepository(
    val jdbcTemplate: JdbcTemplate
) {
    private val rowMapper = StreamRowMapper()

    fun findById(streamUrl: String): StreamEntity? {
        val sql = """
            select * from stream
            where id = ?
        """.trimIndent()
        return jdbcTemplate.queryForObject(sql, rowMapper, streamUrl)
    }

    fun deleteIfTerminated(streamUrl: String) {
        val sql = """
            delete from stream where id = ?
        """.trimIndent()
        jdbcTemplate.update(sql, streamUrl)
    }

    fun upsert(streamUrl: String): StreamEntity? {
        val sql = """
            insert into stream (id, state)
            values (?, ?)
            on conflict (id) do nothing
            returning *
        """.trimIndent()
        return jdbcTemplate.queryForObject(sql, rowMapper, streamUrl, StreamState.IN_PROGRESS.name)
    }

    fun updateState(streamURL: String, newState: StreamState) {
        val sql = """
            update stream
            set state = ?
            where id = ?
        """.trimIndent()
        jdbcTemplate.update(sql, newState.toString(), streamURL)
    }
}