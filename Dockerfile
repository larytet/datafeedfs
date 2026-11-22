# datafeedfs image, Go implementation: static Go binary + the bundled Nasdaq ITCH parser, no Python. Build from the repository root:
#   docker build -t datafeedfs-go .
#   docker build --target test -t datafeedfs-go-test . && docker run --rm datafeedfs-go-test
FROM debian:bookworm-slim AS parser-build
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev && rm -rf /var/lib/apt/lists/*
COPY parsers/itch_tob.c /src/itch_tob.c
RUN gcc -O2 -Wall -o /src/itch_tob /src/itch_tob.c

FROM golang:1.26-bookworm AS go-build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY src ./src
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/datafeedfs-go ./src

FROM go-build AS test
COPY parsers ./parsers
RUN go vet ./...
CMD ["go", "test", "./..."]

FROM debian:bookworm-slim AS runtime
# fuse3: fusermount3 used by go-fuse; openssh-client: reaches the pcap host; xz-utils / zstd: pcap decompression; bash: the parser script
RUN apt-get update && apt-get install -y --no-install-recommends fuse3 openssh-client xz-utils zstd bash ca-certificates \
    && rm -rf /var/lib/apt/lists/* && mkdir -p /root/.ssh /cache /config /mnt/datafeed && chmod 700 /root/.ssh \
    && echo user_allow_other >> /etc/fuse.conf
WORKDIR /app
COPY parsers ./parsers
COPY --from=parser-build /src/itch_tob ./parsers/itch_tob
COPY --from=go-build /out/datafeedfs-go /usr/local/bin/datafeedfs-go
COPY config.docker.toml /app/config.docker.toml
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
ENV DFS_CONFIG=/config/config.toml DFS_MOUNTPOINT=/mnt/datafeed
VOLUME ["/cache"]
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["mount"]

# last stage = default target of a plain docker build
FROM runtime AS final
