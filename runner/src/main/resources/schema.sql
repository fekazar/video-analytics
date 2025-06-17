create table if not exists stream(
    id text primary key,
    state text not null,
    streamUrl text unique not null,
    chunksBucket text,
    updatedAt timestamp not null
);

create index if not exists find_by_stream_url_idx on stream(streamUrl);
