FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY storage ./storage
RUN go test ./... && CGO_ENABLED=0 go build -trimpath -o /marchiq ./cmd/marchiq

FROM alpine:3.22
RUN addgroup -S marchiq && adduser -S -G marchiq marchiq && \
    mkdir /data && chown marchiq:marchiq /data
COPY --from=build /marchiq /usr/local/bin/marchiq
USER marchiq
EXPOSE 9092
ENTRYPOINT ["marchiq"]
CMD ["-dataDir", "/data", "-addr", ":9092"]
