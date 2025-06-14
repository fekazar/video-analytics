package edu.misis.runner

import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Repository
import java.sql.ResultSet
import kotlin.jvm.optionals.getOrNull

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
    val jdbcClient: JdbcClient
) {
    private val rowMapper = StreamRowMapper()

    fun findById(streamUrl: String): StreamEntity? {
        val sql = """
            select * from stream
            where id = ?
        """.trimIndent()
        return jdbcClient.sql(sql)
            .params(streamUrl)
            .query(rowMapper)
            .optional()
            .getOrNull()
    }

    fun deleteIfTerminated(streamUrl: String) {
        val sql = """
            delete from stream where id = ? and state = ?
        """.trimIndent()
        //jdbcTemplate.update(sql, streamUrl)
        jdbcClient.sql(sql)
            .params(streamUrl, StreamState.TERMINATED.name)
            .update()
    }

    fun upsert(streamUrl: String): StreamEntity? {
        val sql = """
            insert into stream (id, state)
            values (?, ?)
            on conflict (id) do nothing
            returning *
        """.trimIndent()
        //return jdbcTemplate.queryForObject(sql, rowMapper, streamUrl, StreamState.IN_PROGRESS.name)
        return jdbcClient.sql(sql)
            .params(streamUrl, StreamState.IN_PROGRESS.name)
            .query(rowMapper)
            .optional()
            .getOrNull()
   }

    fun updateState(streamUrl: String, newState: StreamState) {
        val sql = """
            update stream
            set state = ?
            where id = ?
        """.trimIndent()
        //jdbcTemplate.update(sql, newState.toString(), streamUrl)
        jdbcClient.sql(sql)
            .params(newState.name, streamUrl)
            .update()
    }
}