# datafeedfs image, Python implementation: FUSE filesystem + the bundled Nasdaq ITCH parser. Build from the repository root:
#   docker build -f py/Dockerfile -t datafeedfs .          (the last stage below is the runtime image, so this is the default)
#   docker build -f py/Dockerfile --target test -t datafeedfs-test . && docker run --rm --device /dev/fuse --cap-add SYS_ADMIN --security-opt apparmor:unconfined datafeedfs-test
FROM python:3.11-slim-bookworm AS parser-build
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev && rm -rf /var/lib/apt/lists/*
COPY parsers/itch_tob.c /src/itch_tob.c
RUN gcc -O2 -Wall -o /src/itch_tob /src/itch_tob.c

FROM python:3.11-slim-bookworm AS runtime
# fuse: fusermount for unmounting; libfuse2: what fusepy loads; openssh-client: reaches the pcap host; xz-utils / zstd: pcap decompression
RUN apt-get update && apt-get install -y --no-install-recommends fuse libfuse2 openssh-client xz-utils zstd \
    && rm -rf /var/lib/apt/lists/* && mkdir -p /root/.ssh /cache /config /mnt/datafeed && chmod 700 /root/.ssh
WORKDIR /app
COPY py/requirements.txt py/requirements.txt
RUN pip install --no-cache-dir -r py/requirements.txt
COPY py ./py
COPY parsers ./parsers
COPY --from=parser-build /src/itch_tob ./parsers/itch_tob
COPY py/config.docker.toml /app/config.docker.toml
COPY py/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
ENV DFS_CONFIG=/config/config.toml DFS_MOUNTPOINT=/mnt/datafeed PYTHONUNBUFFERED=1
VOLUME ["/cache"]
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["mount"]

FROM runtime AS test
COPY py/requirements-dev.txt py/requirements-dev.txt
RUN pip install --no-cache-dir -r py/requirements-dev.txt
WORKDIR /app/py
ENTRYPOINT ["python", "-m", "pytest"]
CMD ["-q"]

# last stage = default target of a plain docker build
FROM runtime AS final
