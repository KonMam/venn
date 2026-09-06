# Built by goreleaser (dockers_v2), which supplies the cross-compiled binaries
# as build context and sets TARGETOS/TARGETARCH per platform. Static and
# CGO-free, so the runtime stage needs nothing but certs.
FROM alpine:3.20 AS certs
RUN apk add --no-cache ca-certificates

FROM scratch
ARG TARGETOS
ARG TARGETARCH
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY venn_${TARGETOS}_${TARGETARCH}*/venn /venn
ENTRYPOINT ["/venn"]
