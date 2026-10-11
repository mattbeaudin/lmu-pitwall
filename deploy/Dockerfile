# LMU Pitwall relay — the same binary as the Windows product, run with --server.
#
# Packages a prebuilt `make build-server` binary rather than compiling here: the
# dashboard has to be built and embedded first (bridge/src/assets.rs), and that
# toolchain already lives on the development machine.
#
# distroless/cc is glibc + libgcc and nothing else — no shell, no package
# manager — which is exactly what the binary links against (glibc 2.35+; Debian
# 12 ships 2.36). TLS roots are compiled in (webpki-roots), so no CA bundle.
FROM gcr.io/distroless/cc-debian12:nonroot

COPY lmu-pitwall-server /lmu-pitwall-server

EXPOSE 9000
ENTRYPOINT ["/lmu-pitwall-server", "--server"]
