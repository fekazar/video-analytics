package edu.misis.streamer.repository

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.stereotype.Repository
import java.sql.Timestamp
import java.util.UUID

@Repository
class InferenceRepository(
    private val jdbcTemplate: JdbcTemplate,
    private val jdbcClient: JdbcClient,
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

    fun getLastResult(streamId: UUID): List<IntervalAverageProj> {
        val sql = """
            select 
                time_bucket('5 seconds', instant) AS five_second_interval,
                avg(facesCount) AS average_faces_count,
                count(*) as samples_in_interval
            from 
                inference_events
            where streamId = ?
            group by 
                five_second_interval
            order by
                five_second_interval desc 
            limit 60;
        """.trimIndent()
        return jdbcClient.sql(sql)
            .params(streamId.toString())
            .query { rs, _ -> IntervalAverageProj(
                rs.getTimestamp("five_second_interval").toInstant(),
                rs.getTimestamp("five_second_interval").toInstant().plusSeconds(5),
                rs.getDouble("average_faces_count"),
                rs.getInt("samples_in_interval"),
                )
            }.list()
    }
}