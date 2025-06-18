package edu.misis.runner.repository

import edu.misis.runner.StreamState
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Repository
import java.net.URI
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant
import java.util.*
import kotlin.jvm.optionals.getOrNull

private class StreamRowMapper : RowMapper<StreamEntity> {
    override fun mapRow(rs: ResultSet, rowNum: Int): StreamEntity {
        return StreamEntity(
            UUID.fromString(rs.getString("id")),
            StreamState.valueOf(rs.getString("state")),
            URI(rs.getString("streamUrl")),
            rs.getString("chunksBucket"),
            rs.getTimestamp("updatedAt").toInstant(),
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
            and state != 'TERMINATED'
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
            and state != 'TERMINATED'
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
            insert into stream (id, state, streamUrl, updatedAt)
            values (?, ?, ?, ?)
            returning *
        """.trimIndent()
        return jdbcClient.sql(sql)
            .params(UUID.randomUUID(), StreamState.INITIAL.name, streamUrl.toString(), Timestamp.from(Instant.now()),)
            .query(rowMapper)
            .optional()
            .getOrNull()
    }

    fun update(stream: StreamEntity) {
        val sql = """
            update stream
            set state = ?, streamUrl = ?, chunksBucket = ?, updatedAt = ?
            where id = ?
        """.trimIndent()
        jdbcClient.sql(sql)
            .params(
                stream.state.name,
                stream.streamUrl.toString(),
                stream.chunksBucket,
                Timestamp.from(Instant.now()),
                stream.id.toString(),
            ).update()
    }

    fun updateState(id: UUID, newState: StreamState) {
        val sql = """
            update stream
            set state = ?, updatedAt = ?
            where id = ?
        """.trimIndent()
        jdbcClient.sql(sql)
            .params(newState.name, Timestamp.from(Instant.now()), id.toString())
            .update()
    }
}