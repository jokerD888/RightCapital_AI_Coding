FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -o /out/notification ./cmd/server \
    && CGO_ENABLED=0 go build -trimpath -o /out/mockvendor ./cmd/mockvendor

FROM alpine:3.23
RUN apk add --no-cache ca-certificates \
    && adduser -D -u 10001 notification
COPY --from=build /out/ /usr/local/bin/
USER 10001:10001
ENV HTTP_ADDR=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/notification"]
