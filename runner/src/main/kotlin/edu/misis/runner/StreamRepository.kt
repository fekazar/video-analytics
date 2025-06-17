package edu.misis.runner

import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Repository
import java.net.URI
import java.sql.ResultSet
import java.util.UUID
import kotlin.jvm.optionals.getOrNull

private class StreamRowMapper : RowMapper<StreamEntity> {
    override fun mapRow(rs: ResultSet, rowNum: Int): StreamEntity {
        return StreamEntity(
            UUID.fromString(rs.getString("id")),
            StreamState.valueOf(rs.getString("state")),
            URI(rs.getString("streamUrl")),
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

    fun findByStreamUrl(streamUrl: URI): StreamEntity? {
        val sql = """
            select * from stream
            where streamUrl = ?
        """.trimIndent()
        return jdbcClient.sql(sql)
            .params(streamUrl.toString())
            .query(rowMapper)
            .optional()
            .getOrNull()
    }

    fun deleteById(id: UUID) {
        val sql = """
            delete from stream where id = ?
        """.trimIndent()
        jdbcClient.sql(sql)
            .params(id.toString())
            .update()
    }

    fun createNewStream(streamUrl: URI): StreamEntity? {
        val sql = """
            insert into stream (id, state, streamUrl)
            values (?, ?, ?)
            on conflict (streamUrl) do nothing
            returning *
        """.trimIndent()
        return jdbcClient.sql(sql)
            .params(UUID.randomUUID(), StreamState.INITIAL.name, streamUrl.toString())
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
            .params(stream.state.name, stream.streamUrl.toString(), stream.chunksBucket, stream.id.toString())
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