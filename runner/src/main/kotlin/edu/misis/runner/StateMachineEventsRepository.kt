package edu.misis.runner

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.springframework.jdbc.core.RowMapper
import org.springframework.jdbc.core.simple.JdbcClient
import java.sql.ResultSet
import java.time.Instant
import java.util.*

private const val EVENT_PROCESSING_SECONDS = 60L
private const val BATCH_LIMIT = 100

private val jsonMapper = ObjectMapper()
    .registerKotlinModule()

private class EventRowMapper : RowMapper<StateMachineEventEntity> {
    override fun mapRow(rs: ResultSet, rowNum: Int): StateMachineEventEntity {
        return StateMachineEventEntity(
            UUID.fromString(rs.getString("id")),
            Instant.parse(rs.getString("createdAt")),
            rs.getString("type"),
            jsonMapper.readValue<Map<String, String>>(
                rs.getString("headers")
            ),
            Instant.parse(rs.getString("lockedUntil")),
            Instant.parse(rs.getString("processedAt")),
        )
    }
}

class StateMachineEventsRepository(
    private val jdbcClient: JdbcClient
){
    private val rowMapper = EventRowMapper()

    fun insertEvent(event: StateMachineEventEntity) {
        val sql = """
            insert into statemachine_event (id, createdAt, type, headers, lockedUntil)
            values ?, ?, ?, ?
        """.trimIndent()
        jdbcClient.sql(sql)
            .params(
                event.id,
                event.createdAt,
                event.type,
                jsonMapper.writeValueAsString(event.headers),
                Instant.now(),
            )
    }

    // Returns in history order
    fun findUnprocessed(): List<StateMachineEventEntity> {
        val sql = """
            update statemachine_event
            set lockedUntil = ?
            join (
                select *
                from statemachine_event
                where lockedUntil < ?
                and processedAt is null
                order by createdAt
                limit ?
            )
            returning *
        """.trimIndent()
        return jdbcClient.sql(sql)
            .params(Instant.now().plusSeconds(EVENT_PROCESSING_SECONDS), BATCH_LIMIT)
            .query(rowMapper)
            .list()
    }
}