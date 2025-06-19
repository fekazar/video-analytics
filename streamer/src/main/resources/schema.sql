create table if not exists stream(
    id text primary key,
    state text not null,
    streamUrl text not null,
    chunksBucket text,
    updatedAt timestamp not null
);

create index if not exists find_by_stream_url_idx on stream(streamUrl);

create unique index unique_non_terminated_stream_idx on stream(streamUrl)
where state != 'TERMINATED';

create table if not exists inference_events (
    instant timestamp,
    streamId text,
    facesCount integer
) with (
  timescaledb.hypertable,
  timescaledb.partition_column='instant',
  timescaledb.segmentby='streamId'
);
