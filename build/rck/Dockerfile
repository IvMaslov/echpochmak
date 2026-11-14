# syntax=docker/dockerfile:1

# Iceberg's REST Compatibility Kit (RCK): Java's CatalogTests and
# ViewCatalogTests, run through Java's RESTCatalog against a remote server.
#
# Everything is downloaded and compiled at build time, so a test run works
# with --offline. Build with BUILD_NETWORK=host behind a VPN.
#
# Iceberg 1.12.0 is the release closest to our pinned spec: the spec commit in
# api/iceberg/SPEC_VERSION is 86 commits after it, and the two specs differ
# only in one column-statistics field.
FROM eclipse-temurin:21-jdk-noble@sha256:06c1f157dacf6b68198bd9a4186dd4c34ef24e65d7bbbe2a0cf8e62571ad38c9

ARG ICEBERG_VERSION=1.12.0

# The Apache source release, checked against the SHA-256 of the file whose
# published SHA-512 was verified. It carries version.txt, so Gradle needs no
# git history.
ADD --checksum=sha256:78f6b9b87da7f2815627d10db6ef5a6cc5fd8e0710672e29ee9f56a2c9089662 \
    https://archive.apache.org/dist/iceberg/apache-iceberg-${ICEBERG_VERSION}/apache-iceberg-${ICEBERG_VERSION}.tar.gz \
    /tmp/iceberg.tar.gz

RUN useradd --create-home rck \
    && mkdir /rck \
    && tar -xzf /tmp/iceberg.tar.gz -C /rck --strip-components=1 \
    && rm /tmp/iceberg.tar.gz \
    && chown -R rck:rck /rck

USER rck
WORKDIR /rck

COPY --chown=rck:rck build/rck/init.gradle /home/rck/.gradle/init.d/rck.gradle

# Download Gradle and every dependency, compile the suite and build the
# bundles on its runtime classpath: running the test task with a filter that
# matches nothing resolves everything a real run needs.
RUN ./gradlew --no-daemon --quiet :iceberg-open-api:test --tests org.apache.iceberg.rest.WarmUpOnly

COPY --chown=rck:rck build/rck/run.sh /usr/local/bin/run-rck

ENTRYPOINT ["/usr/local/bin/run-rck"]
