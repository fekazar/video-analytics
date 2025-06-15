package edu.misis.runner

import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Repository
import java.sql.ResultSet
import java.util.UUID
import kotlin.jvm.optionals.getOrNull

private class StreamRowMapper : RowMapper<StreamEntity> {
    override fun mapRow(rs: ResultSet, rowNum: Int): StreamEntity {
        return StreamEntity(
            UUID.fromString(rs.getString("id")),
            StreamState.valueOf(rs.getString("state")),
            rs.getString("streamUrl"),
            rs.getString("chunksBucket")
        )
    }
}

@Repository
class StreamRepository(
    private val jdbcClient: JdbcClient
) {
    private val rowMapper = StreamRowMapper()

    fun findById(id: UUID): StreamEntity? {
        val sql = """
            select * from stream
            where id = ?
        """.trimIndent()
        return jdbcClient.sql(sql)
            .params(id.toString())
            .query(rowMapper)
            .optional()
            .getOrNull()
    }

    fun deleteIfTerminated(streamUrl: String) {
        val sql = """
            delete from stream where streamUrl = ? and state = ?
        """.trimIndent()
        jdbcClient.sql(sql)
            .params(streamUrl, StreamState.TERMINATED.name)
            .update()
    }

    fun createNewStream(streamUrl: String): StreamEntity? {
        val sql = """
            insert into stream (id, state, streamUrl)
            values (?, ?, ?)
            on conflict (streamUrl) do nothing
            returning *
        """.trimIndent()
        return jdbcClient.sql(sql)
            .params(UUID.randomUUID(), StreamState.INITIAL.name, streamUrl)
            .query(rowMapper)
            .optional()
            .getOrNull()
    }

    fun update(stream: StreamEntity) {
        val sql = """
            update stream
            set state = ?, streamUrl = ?, chunksBucket = ?
            where id = ?
        """.trimIndent()
        jdbcClient.sql(sql)
            .params(stream.state.name, stream.streamUrl, stream.chunksBucket, stream.id.toString())
            .update()
    }

    fun initTermination(streamUrl: String) {
        val sql = """
            update stream
            set state = ?
            where streamUrl = ?
        """.trimIndent()
        jdbcClient.sql(sql)
            .params(StreamState.AWAIT_TERMINATION.name, streamUrl)
            .update()
    }

    fun updateState(id: UUID, newState: StreamState) {
        val sql = """
            update stream
            set state = ?
            where id = ?
        """.trimIndent()
        jdbcClient.sql(sql)
            .params(newState.name, id.toString())
            .update()
    }
}