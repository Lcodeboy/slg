#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION=3.9.11
SHA512=bcfe4fe305c962ace56ac7b5fc7a08b87d5abd8b7e89027ab251069faebee516b0ded8961445d6d91ec1985dfe30f8153268843c89aa392733d1a3ec956c9978
if [[ -n "${MVN_COMMAND:-}" ]]; then
  MAVEN="$MVN_COMMAND"
elif command -v mvn >/dev/null 2>&1; then
  MAVEN="$(command -v mvn)"
elif [[ -x '/Applications/IntelliJ IDEA CE.app/Contents/plugins/maven/lib/maven3/bin/mvn' ]]; then
  MAVEN='/Applications/IntelliJ IDEA CE.app/Contents/plugins/maven/lib/maven3/bin/mvn'
else
  MAVEN="$ROOT/.tools/apache-maven-$VERSION/bin/mvn"
  if [[ ! -x "$MAVEN" ]]; then
    mkdir -p "$ROOT/.tools"
    ARCHIVE="$ROOT/.tools/maven-$VERSION.tar.gz"
    curl --fail --location --retry 3 "https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/$VERSION/apache-maven-$VERSION-bin.tar.gz" -o "$ARCHIVE"
    if command -v sha512sum >/dev/null 2>&1; then
      ACTUAL="$(sha512sum "$ARCHIVE" | cut -d ' ' -f 1)"
    else
      ACTUAL="$(shasum -a 512 "$ARCHIVE" | cut -d ' ' -f 1)"
    fi
    [[ "$ACTUAL" == "$SHA512" ]] || { echo 'Maven download checksum mismatch' >&2; exit 1; }
    tar -xzf "$ARCHIVE" -C "$ROOT/.tools"
    rm "$ARCHIVE"
  fi
fi
exec "$MAVEN" -s "$ROOT/.mvn/settings.xml" "-Dmaven.repo.local=$ROOT/.cache/m2" "$@"
