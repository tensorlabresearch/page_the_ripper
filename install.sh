#!/usr/bin/env bash
#
# Install (or upgrade) Page the Ripper from the latest GitHub release.
#
# One-liner:
#   curl -fsSL https://raw.githubusercontent.com/kai5263499/page_the_ripper/main/install.sh | sudo bash
#
set -euo pipefail

REPO="kai5263499/page_the_ripper"
TMP_DEB="/tmp/page-the-ripper.deb"

echo "==> Page the Ripper installer"

# Remove old package if installed
if dpkg -l page-the-ripper >/dev/null 2>&1; then
    echo "==> Removing existing page-the-ripper package..."
    apt-get remove -y page-the-ripper
fi

# Fetch the latest release .deb URL from GitHub
echo "==> Fetching latest release from github.com/${REPO}..."
DEB_URL=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
    | grep -oP '"browser_download_url":\s*"\K[^"]+\.deb')

if [ -z "$DEB_URL" ]; then
    echo "ERROR: Could not find .deb in latest release." >&2
    exit 1
fi

echo "==> Downloading ${DEB_URL}..."
curl -fsSL -o "$TMP_DEB" "$DEB_URL"

# Install the package and resolve dependencies
echo "==> Installing page-the-ripper..."
dpkg -i "$TMP_DEB" || true
apt-get install -f -y

rm -f "$TMP_DEB"

echo ""
echo "==> Service status:"
systemctl status page-the-ripper --no-pager || true
echo ""
echo "Done. Page the Ripper is installed and running on port 8000."
