# Built by goreleaser (dockers_v2), which stages the cross-compiled binary in
# the build context at <os>/<arch>/venn and sets TARGETOS/TARGETARCH per
# platform. Static and CGO-free, so the runtime stage needs nothing but certs.
FROM --platform=$BUILDPLATFORM alpine:3.20 AS certs
RUN apk add --no-cache ca-certificates

FROM scratch
ARG TARGETOS
ARG TARGETARCH
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY ${TARGETOS}/${TARGETARCH}/venn /venn
ENTRYPOINT ["/venn"]
