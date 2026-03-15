#!/usr/bin/env bash
#
# Build the page-the-ripper .deb package.
# Usage: debian/build.sh [version]
#   version defaults to the Version field in debian/control.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
cd "$REPO_DIR"

# Read version from control file or CLI arg
VERSION="${1:-$(grep '^Version:' debian/control | awk '{print $2}')}"
PKG="page-the-ripper"
STAGE="debian/build/${PKG}_${VERSION}_all"

echo "Building ${PKG} ${VERSION} ..."

# Clean previous build
rm -rf debian/build
mkdir -p "$STAGE"

# -- Application code --
mkdir -p "$STAGE/opt/page-the-ripper/tools"
cp main.py scan_local.py "$STAGE/opt/page-the-ripper/"
cp scanner.cfg.example "$STAGE/opt/page-the-ripper/"
cp debian/requirements-runtime.txt "$STAGE/opt/page-the-ripper/requirements.txt"

# tools/ directory
cp tools/escl_scan.py tools/scan_cli.py "tools/_escl-scan.py" \
   "$STAGE/opt/page-the-ripper/tools/"

# Create __init__.py for proper Python package imports
touch "$STAGE/opt/page-the-ripper/tools/__init__.py"

# -- Config directory (conffiles) --
mkdir -p "$STAGE/etc/page-the-ripper"
cp scanner.cfg.example "$STAGE/etc/page-the-ripper/scanner.cfg"

# -- Scanner detection script --
mkdir -p "$STAGE/usr/lib/page-the-ripper"
cp debian/detect-scanners "$STAGE/usr/lib/page-the-ripper/detect-scanners"
chmod 755 "$STAGE/usr/lib/page-the-ripper/detect-scanners"

# -- Systemd unit --
mkdir -p "$STAGE/lib/systemd/system"
cp debian/page-the-ripper.service "$STAGE/lib/systemd/system/page-the-ripper.service"

# -- Runtime data directory --
mkdir -p "$STAGE/var/lib/page-the-ripper/scans"

# -- DEBIAN control files --
mkdir -p "$STAGE/DEBIAN"
cp debian/control "$STAGE/DEBIAN/control"
cp debian/conffiles "$STAGE/DEBIAN/conffiles"

for script in postinst prerm postrm; do
    cp "debian/$script" "$STAGE/DEBIAN/$script"
    chmod 755 "$STAGE/DEBIAN/$script"
done

# Stamp version if passed as argument
if [ "${1:-}" != "" ]; then
    sed -i "s/^Version:.*/Version: $VERSION/" "$STAGE/DEBIAN/control"
fi

# -- Build --
dpkg-deb --build "$STAGE"

DEB="${STAGE}.deb"
echo ""
echo "Package built: $DEB"
echo "Size: $(du -h "$DEB" | cut -f1)"
echo ""
dpkg-deb -I "$DEB"
