# wan-recover

A small Rust service that watches a UniFi gateway's primary WAN and restarts a
Meross-powered modem when the gateway fails over to a working backup connection.

A typical setup is a fibre/nbn modem as primary and Starlink as backup. A normal
internet probe still succeeds after failover, so this service reads the gateway's
WAN-specific health instead. Monitoring and relay control run entirely over the
local network; the service does not need a Meross cloud session.

**Experimental. Disabled by default.** The gateway adapter uses an internal
UniFi API. Verify your interfaces and outlet identity before enabling it. Never
connect the gateway, Wi-Fi access point, backup modem, or service host to the
controlled outlet.

## How it works

1. Every five seconds, read the gateway's WAN health through a restricted SSH command.
2. Require the primary WAN to report bad health while a healthy backup is active
   for 90 seconds. Monitoring errors and ambiguous or stale state cancel the timer.
3. Verify the Meross UUID and channel, confirm the outlet is already on, and
   recheck the gateway immediately before acting.
4. Persist a pending power-on marker, turn the modem off for 15 seconds, then
   turn it on and verify the result. Retry power-on if communication fails.

Defaults allow one attempt per outage, a 30-minute cooldown, and three cycles
per rolling day. Five minutes of observed primary recovery re-arms the service.
The journal preserves rate limits and unfinished power-on operations across
restarts. Pending power-on recovery runs even when new cycles are disabled.

The service does not change UniFi's WAN configuration or manage failback.

## Compatibility and validation

The initial installation used a Dream Machine Pro with UniFi OS core 5.1.132,
Network 10.6.106, and an MSS310 with hardware 8.0.0 / firmware 8.3.5. Live gateway
reads, signed local relay reads, and an idempotent ON command have been verified.
A complete real outage → power cycle → restored WAN sequence has not yet been
observed. Failure fixtures are synthetic mutations of a normalized healthy capture.

The adapter expects the `wanFailover` service returned by
`ubios-udapi-client -r GET /services`, plus timestamps and link state from
`GET /interfaces`. Unexpected schema or firmware changes inhibit new cycles.
Other UniFi gateways and Meross models are unverified. Meross support is limited
to signed local HTTP `Appliance.Control.ToggleX` with a legitimate device key;
legacy Toggle, Matter-only, and encrypted-LAN-only protocols are unsupported.

## Run locally

Requires Rust 1.93.1, OpenSSH at `/usr/bin/ssh`, and a Linux or macOS host with
access to both LAN devices. The gateway helper requires Python 3 and
`ubios-udapi-client` on the gateway. The repository includes a mise tool pin.

```sh
mise trust
mise x -- cargo test --locked
cp config.example.toml config.toml
mkdir -p secrets data
chmod 700 secrets
openssl rand -hex 32 > secrets/webhook-token
chmod 600 secrets/webhook-token
mise x -- cargo run --locked -- config.toml
```

This starts in observation mode on `127.0.0.1:8080`. Configure `[gateway]` to
start live monitoring. An unconfigured reader does not monitor your network.
Use a private listen address or firewall when exposing the HTTP port on the LAN.

### Gateway reader

Identify the primary and backup interface names and their metrics from your
actual gateway. Do not assume every gateway uses `eth8` and `eth9`. Verify the
SSH host-key fingerprint through an independent trusted channel and store that
host's public key in `secrets/known_hosts`.

Open a temporary administrator SSH master using that trusted host-key file:

```sh
ssh -M -S /tmp/wan-recover-admin -o ControlPersist=300 \
  -o StrictHostKeyChecking=yes -o UserKnownHostsFile=secrets/known_hosts \
  -fN root@10.0.0.1
python3 scripts/install-gateway.py \
  --control-path /tmp/wan-recover-admin --gateway 10.0.0.1 --node 10.0.0.20
ssh -S /tmp/wan-recover-admin -O exit root@10.0.0.1
```

Replace both example addresses. `--node` must be the source IP the gateway sees
from the service. The installer generates a dedicated key and adds a
`from=...,restrict,command=...` authorized-key entry. That key can only execute
`gateway/wan-health.py`; it cannot request arbitrary commands or forwarding.
Existing authorized keys are preserved. The app never needs the administrator
password. The helper performs only GET operations and emits a small projection
without the rest of the gateway configuration.

Set `[gateway]` in `config.toml`, including your gateway address, interface names,
`secrets/gateway-key`, and `secrets/known_hosts`. Recheck the helper and key after
firmware upgrades or SSH configuration changes; their persistence is not guaranteed.

### Meross outlet

Obtain your legitimate local device key and UUID using your existing Meross
integration or authenticated provisioning tools such as
[MerossIot](https://github.com/albertogeniola/MerossIot). Cloud login/key retrieval
is not implemented in this service. The local protocol follows the signed HTTP
format used by [meross_lan](https://github.com/krahabb/meross_lan/tree/master/custom_components/meross_lan/merossclient).

Save the key in `secrets/meross-key`, restrict its permissions, and configure
`[meross]` with the plug's reserved LAN IP, verified UUID, and modem-only channel.
Then perform a read-only check:

```sh
mise x -- cargo run --locked -- config.toml check-relay
```

Only after verifying the target and gateway observations should you set
`enabled = true` and restart the service. Arrange a controlled acceptance test
or inspect the first real outage to validate the complete recovery sequence on
your hardware. Never use a fabricated production alarm to bypass the WAN guard.

## Containers and Kubernetes

```sh
docker build -t wan-recover:local .
```

For Linux Docker, use host networking, a read-only root filesystem, and writable
persistent storage for `/data`. Set these paths in your mounted configuration:
`data_dir = "/data"`, `token_file = "/run/wan-recover/webhook-token"`, and secret
paths under `/run/wan-recover/`. The container runs as UID/GID 65532; give that
user access to its data directory and secrets. Docker Desktop host networking
may differ from a native Linux host; verify device reachability.

```sh
# Prepare writable storage and a container-specific config.toml first.
docker run --rm --name wan-recover --network host --read-only --cap-drop ALL \
  --security-opt no-new-privileges \
  --mount type=bind,src="$PWD/config.toml",dst=/etc/wan-recover/config.toml,readonly \
  --mount type=bind,src="$PWD/secrets",dst=/run/wan-recover,readonly \
  --mount type=bind,src="$PWD/data",dst=/data \
  wan-recover:local
```

[deploy/kubernetes](deploy/kubernetes) contains a portable, single-replica example
with `hostNetwork: true`, `ClusterFirstWithHostNet`, Recreate, health probes, a
PVC, and a non-root container. Copy its example config to `config.toml`, configure
your devices, and set an image your cluster can pull or load. Keep observation
mode until integration checks pass.

```sh
cp deploy/kubernetes/config.example.toml deploy/kubernetes/config.toml
# Edit the config and image before applying. Select durable storage for the PVC.
kubectl apply -f deploy/kubernetes/namespace.yaml
kubectl -n wan-recover create secret generic wan-recover-secrets \
  --from-file=webhook-token=secrets/webhook-token \
  --from-file=gateway-key=secrets/gateway-key \
  --from-file=known_hosts=secrets/known_hosts \
  --from-file=meross-key=secrets/meross-key
kubectl apply -k deploy/kubernetes
kubectl -n wan-recover rollout status deployment/wan-recover
```

Use your intended kube context. The example PVC uses the default StorageClass;
choose suitable retention, backup, and volume ownership settings. Host networking
requires an admission-policy exception for the dedicated namespace, but the
container remains unprivileged. Pin the pod to the node whose source IP is
allowed by the gateway key, or deliberately configure restrictions for every
eligible node. Keep exactly one instance and preserve its journal during upgrades.

## Operations

- `GET /healthz`: HTTP process health.
- `GET /readyz`: worker heartbeat, including during a power cycle. This does not
  prove that either external integration is reachable.
- Authenticated `GET /status`: enabled state, current gateway observation,
  last successful read, outage timer, cycle count, last action, and pending ON.

Use `Authorization: Bearer <webhook-token>`. Keep this endpoint and the token on
trusted networks. Local Meross HTTP is not transport-encrypted.

Set `enabled = false` and restart/redeploy to stop new cycles. Monitoring
continues. Wait for any pending ON to finish before stopping the process. A
cluster failure or loss of Wi-Fi/LAN while the outlet is off can delay power-on;
a two-command relay cycle cannot be physically atomic.

Authenticated `/hooks/unifi` and `/hooks/unifi/{token}` routes remain available
for diagnostic alarms. Their example JSON mapping is not a verified UniFi
webhook schema. When the gateway reader is configured, webhook events cannot
change policy state. Observation-only `/last-webhook` captures one bounded body
in memory; it is disabled in enabled mode.

## Development

```sh
mise x -- cargo fmt --check
mise x -- cargo test --locked
mise x -- cargo clippy --all-targets --locked -- -D warnings
```

The 36 tests cover policy/replay behavior, durable state, authenticated HTTP,
Meross signatures/identity, ambiguous replies, ON retries, gateway freshness,
preflight cancellation, and monitoring gaps. Hardware switching tests use fakes.
CI runs the same checks and builds the container.

Reports with sanitized gateway snapshots or device model/firmware details are
welcome. Do not attach keys, passwords, bearer tokens, raw cloud responses, or
household configuration. Contributions should include tests for new behavior.

Licensed under [MIT](LICENSE).
