#!/bin/sh
# Runs the REST Compatibility Kit against the catalog named by CATALOG_URI.
# Reports are left in open-api/build/{test-results,reports}/test, where
# `make rck` copies them from. Extra arguments go to Gradle, for example
# --tests '*ViewCatalogTests.renameView' to run one test.
#
# The server supports names with dots and the v3 variant type, so the tests of
# both run; the kit leaves them off by default.
#
# The two test classes are selected directly rather than through
# RESTCompatibilityKitSuite: Gradle's exclude filters, which the exclusion
# list becomes, do not apply inside a JUnit suite.
set -u

# Wait for the catalog: the stack may still be starting.
for i in $(seq 60); do
    curl -fsS "${CATALOG_URI%/}/v1/config" >/dev/null 2>&1 && break
    [ "$i" -eq 60 ] && { echo "catalog at $CATALOG_URI is not answering" >&2; exit 1; }
    sleep 1
done

exec ./gradlew --offline --no-daemon --console=plain \
    :iceberg-open-api:test \
    --tests org.apache.iceberg.rest.RESTCompatibilityKitCatalogTests \
    --tests org.apache.iceberg.rest.RESTCompatibilityKitViewCatalogTests \
    -Drck.local=false \
    -Drck.requires-namespace-create="${RCK_REQUIRES_NAMESPACE_CREATE:-true}" \
    -Drck.supports-serverside-retry="${RCK_SUPPORTS_SERVERSIDE_RETRY:-true}" \
    -Drck.supports-names-with-dot=true \
    -Drck.supports-variant=true \
    "$@"
