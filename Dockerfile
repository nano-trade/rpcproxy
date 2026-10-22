# Build a static binary, then copy it into a minimal non-root image.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /rpcproxy .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /rpcproxy /usr/local/bin/rpcproxy
COPY config.banano.json /etc/rpcproxy/config.json
USER nonroot:nonroot
EXPOSE 9970 9972 9978
ENTRYPOINT ["/usr/local/bin/rpcproxy"]
CMD ["-config", "/etc/rpcproxy/config.json"]
