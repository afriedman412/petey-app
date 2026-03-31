#!/bin/bash
# Build Petey Desktop for macOS.
#
# Run from petey-web/:
#   bash desktop/build.sh

set -e

cd "$(dirname "$0")/.."

echo "Installing pyinstaller..."
pip install pyinstaller

echo "Building Petey Desktop..."
pyinstaller desktop/petey.spec --noconfirm

echo ""
echo "Done! App is at: dist/Petey/"
echo "Run with: ./dist/Petey/Petey"
