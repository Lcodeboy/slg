Public disposable test credentials ONLY. Never use in any deployed service. Tests verify mutual TLS and hostname verification with these fixtures. The signing key is not retained.
game-ca.pem is a separate test root. Only game-client.pem is signed by it. The dedicated root's signing key was discarded.
