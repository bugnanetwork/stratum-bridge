FROM golang:1.19.1 as builder

LABEL org.opencontainers.image.description="Dockerized Bugna Stratum Bridge"      
LABEL org.opencontainers.image.authors="onemorebsmith"  
LABEL org.opencontainers.image.source="https://github.com/onemorebsmith/bugna-stratum-bridge"
              
WORKDIR /go/src/app
ADD go.mod .
ADD go.sum .
RUN go mod download

ADD . .
RUN go build -o /go/bin/app ./cmd/bugnabridge


FROM gcr.io/distroless/base:nonroot
COPY --from=builder /go/bin/app /
COPY cmd/bugnabridge/config.yaml /

WORKDIR /
ENTRYPOINT ["/app"]
