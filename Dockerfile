# syntax=docker/dockerfile:1
FROM python:3.12-slim

WORKDIR /app

# Proxy support — passed through automatically from the build environment
ARG http_proxy
ARG https_proxy
ARG no_proxy
ENV http_proxy=$http_proxy \
    https_proxy=$https_proxy \
    no_proxy=$no_proxy

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# devicetype-library (device-types YAMLs only) for /types/library, baked in
# so the container needs no internet access. Pinned: bump the ref deliberately
# so enrichment results are reproducible. VENDORS = vendor directories to keep
# (comma-separated, as named in the library); empty keeps all (~11 MB).
# TEMPORARY: the maintainer's fork, branch "prerelease" = upstream master plus our
# pending PRs (#4637-#4644 merged; pending: netbox-community/devicetype-library#4714,
# Fortinet FAC-3000F).
# Pin its head SHA, not the branch name: an unchanged build arg would keep the
# cached layer forever. The branch gets rebuilt when upstream merges, so bump
# to the new head deliberately. Back to upstream once everything is merged:
# ARG DEVICETYPE_LIBRARY_REPO=netbox-community/devicetype-library
ARG DEVICETYPE_LIBRARY_REPO=Bierchermuesli/devicetype-library
ARG DEVICETYPE_LIBRARY_REF=5f9ba167cff363718f1f12c02d9df10054b8338c
ARG DEVICETYPE_LIBRARY_VENDORS=Cisco,Fortinet
RUN python - <<'PY'
import os, tarfile, urllib.request
repo = os.environ["DEVICETYPE_LIBRARY_REPO"]
ref = os.environ["DEVICETYPE_LIBRARY_REF"]
vendors = {v.strip().lower() for v in os.environ.get("DEVICETYPE_LIBRARY_VENDORS", "").split(",") if v.strip()}
url = f"https://codeload.github.com/{repo}/tar.gz/{ref}"
n = 0
# streamed (the tarball is ~900 MB with images; only device-types/ is kept)
with urllib.request.urlopen(url, timeout=300) as resp, tarfile.open(fileobj=resp, mode="r|gz") as tar:
    for m in tar:
        parts = m.name.split("/", 1)
        path = parts[1].split("/") if len(parts) == 2 else []
        if len(path) >= 3 and path[0] == "device-types" and m.isfile() \
                and (not vendors or path[1].lower() in vendors):
            m.name = parts[1]
            tar.extract(m, "/opt/devicetype-library", filter="data")
            n += 1
print(f"devicetype-library {repo}@{ref} ({', '.join(sorted(vendors)) or 'all vendors'}): {n} files")
if not n:
    raise SystemExit("no device-types extracted: check DEVICETYPE_LIBRARY_VENDORS")
PY

# Swagger UI for /docs, baked in like the library: FastAPI would load it from
# a CDN, which an air-gapped deployment can't reach. Pinned and checksummed.
ARG SWAGGER_UI_VERSION=5.17.14
ARG SWAGGER_UI_SHA256=c57badf459aa6e65cc036b3862d0502a63f9a22546407ffcb0e64f85f816bb28
RUN python - <<'PY'
import hashlib, io, os, tarfile, urllib.request
version, sha = os.environ["SWAGGER_UI_VERSION"], os.environ["SWAGGER_UI_SHA256"]
url = f"https://registry.npmjs.org/swagger-ui-dist/-/swagger-ui-dist-{version}.tgz"
data = urllib.request.urlopen(url, timeout=120).read()
if hashlib.sha256(data).hexdigest() != sha:
    raise SystemExit(f"swagger-ui-dist {version}: sha256 mismatch")
os.makedirs("/app/static/swagger", exist_ok=True)
with tarfile.open(fileobj=io.BytesIO(data)) as tar:
    for name in ("swagger-ui-bundle.js", "swagger-ui.css", "favicon-32x32.png"):
        with open(f"/app/static/swagger/{name}", "wb") as f:
            f.write(tar.extractfile(f"package/{name}").read())
print(f"swagger-ui-dist {version}: /app/static/swagger")
PY
COPY server.py typesync.py jobs.py config.py metrics.py ./
COPY discobox/ discobox/
COPY templates/ templates/


ENTRYPOINT ["python", "server.py"]
