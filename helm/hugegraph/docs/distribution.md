# Chart distribution and offline installation

The OCI chart is named `hugegraph-helm`, independently of the HugeGraph service
version. Its source remains in `helm/hugegraph`. The application name, default
resource names and `nameOverride` / `fullnameOverride` behavior remain unchanged.
Helm 3.8+ is required for standard OCI support.

This distribution starts with HugeGraph **1.8.0**. It does not package charts for
1.7.0 or earlier service releases. Until 1.8.0 images have been released, use a
development chart with the current development images.

## Choose a version and registry

The same packaged chart is published to both registries:

| Registry | Chart reference | Typical use |
| --- | --- | --- |
| GHCR | `oci://ghcr.io/hugegraph/hugegraph-helm` | Public CI |
| Docker Hub | `oci://registry-1.docker.io/hugegraph/hugegraph-helm` | Alternative public entry point |

Always choose an explicit chart version. Public access does not guarantee that
either registry is free from rate limits or temporary outages. The commands below
use example versions; choose a version that has actually been published.

```bash
# Public CI: choose a published development build while service 1.8.0 is unreleased.
CHART_VERSION=0.2.0-dev.123
helm pull oci://ghcr.io/hugegraph/hugegraph-helm --version "$CHART_VERSION"
helm install hugegraph oci://ghcr.io/hugegraph/hugegraph-helm \
  --version "$CHART_VERSION" -n hugegraph --create-namespace --wait --timeout 15m

# Docker Hub: the same chart and version.
helm pull oci://registry-1.docker.io/hugegraph/hugegraph-helm \
  --version "$CHART_VERSION"
```

Development packages append a workflow run number to the source chart version,
for example `0.2.0-dev.123`. They may use `latest` service images. Helm OCI tags
come from the chart's SemVer version; `helm-dev` is not a chart version.

Formal packages use the exact `Chart.version` maintained in source, for example
`0.2.0`. They are published only after the corresponding HugeGraph service release
exists and the chart selects published fixed component images. A source tag
such as `helm-1.8.0` therefore denotes an already released service, not an
advance preview. Chart fixes may be released independently from a reviewed master
commit after the service release; chart and service version numbers need not match.

The manual workflow in [hugegraph/actions](https://github.com/hugegraph/actions)
resolves its selected source ref to a fixed commit, validates and packages the
chart, and installs that exact `.tgz` for its Kubernetes checks before publishing.
Turning off `publish` runs validation without uploading to a registry.

## Install and mirror a chart offline

On an internet-connected machine, download the exact version and the required
container images. Transfer the `.tgz` and images through your normal offline
import process. A chart archive alone does not contain the container images.

```bash
CHART_VERSION=0.2.0-dev.123
helm pull oci://ghcr.io/hugegraph/hugegraph-helm --version "$CHART_VERSION"

# Direct installation from the transferred archive needs no public chart access.
helm install hugegraph "./hugegraph-helm-${CHART_VERSION}.tgz" \
  -n hugegraph --create-namespace -f private-images.yaml --wait --timeout 15m

# Alternatively, import the unchanged archive into a private OCI registry.
helm registry login registry.internal.example.com
helm push "hugegraph-helm-${CHART_VERSION}.tgz" \
  oci://registry.internal.example.com/charts
helm pull oci://registry.internal.example.com/charts/hugegraph-helm \
  --version "$CHART_VERSION"
```

`helm push` takes the parent registry path: Helm adds the packaged chart name and
version. For a private HTTP Helm repository, use that server's upload mechanism
and update its `index.yaml` (for a static repository, `helm repo index` creates the
index). `helm push` is the OCI operation, not a generic HTTP upload command. The
community does not need to provide a separate HTTP chart repository for this flow.

Mirror the configured `hugegraph/pd`, `hugegraph/store`, `hugegraph/server` and,
when enabled, `hugegraph/hubble` images at their selected tags or digests. The
default Store wait container and Helm test hook reuse component images. Explicit
`store.waitImage` / `server.waitImage` overrides must also be mirrored. Use
`global.imageRegistry`, per-component repositories and `imagePullSecrets` as
shown in the [private registry example](../examples/values-internal-registry.yaml).
The mirror must preserve the selected image references and support the cluster's
CPU architecture.
