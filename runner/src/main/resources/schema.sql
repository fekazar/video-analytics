create table if not exists stream(
    id text primary key,
    state text,
    streamUrl text,
    chunksBucket text
);

create index if not exists terminated_stream_idx on stream(id) where state = 'TERMINATED';
