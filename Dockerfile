ARG DEBIAN_FRONTEND=noninteractive

FROM golang:1.24 AS builder
ARG DEBIAN_FRONTEND
RUN groupadd -r pebbletest && useradd -r -g pebbletest pebbletest --home-dir=/app \
    && mkdir /app

WORKDIR /app
COPY . .
RUN chown -R pebbletest:pebbletest /app

USER pebbletest
ENV PATH=/app/bin:$PATH
RUN make build

FROM gcr.io/distroless/base-debian12
COPY --from=builder /app/bin/pebbletest /
CMD ["/pebbletest"]
