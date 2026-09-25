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
# TEMPORARY: the user's fork, branch "prerelease" = upstream master plus our
# pending PRs (netbox-community/devicetype-library#4637-#4644: APs, WP-WIFI6-E,
# Cisco/Fortinet switches, routers, appliances).
# Pin its head SHA, not the branch name: an unchanged build arg would keep the
# cached layer forever. The branch gets rebuilt when upstream merges, so bump
# to the new head deliberately. Back to upstream once everything is merged:
# ARG DEVICETYPE_LIBRARY_REPO=netbox-community/devicetype-library
ARG DEVICETYPE_LIBRARY_REPO=Bierchermuesli/devicetype-library
ARG DEVICETYPE_LIBRARY_REF=32d5159497d32b8e50e416e55ca1d012363407f9
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
COPY discobox.py server.py cli.py typesync.py ./

# Server mode by default; use cli.py for one-shot syncs
ENTRYPOINT ["python", "server.py"]
