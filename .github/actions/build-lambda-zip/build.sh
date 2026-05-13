#!/bin/bash

set -o errexit
set -o nounset

# Parse command line arguments
FOLDER="$1"
PYTHON_VERSION_FILE="$2"
TARGET_DIR="$3"
ZIP_NAME="$4"

# Change to the project folder
cd "$FOLDER"

# Get Python version from .python-version file
if [[ ! -r "$PYTHON_VERSION_FILE" ]]; then
    echo "Error: The file '$PYTHON_VERSION_FILE' does not exist or is not readable." >&2
    exit 1
fi
PY_VERSION=$(cat "$PYTHON_VERSION_FILE")
echo "Using Python version: $PY_VERSION"

# Set up paths
ZIP_TARGET="$TARGET_DIR/$ZIP_NAME"
TMP_DIR="$TARGET_DIR/tmp"

# Ensure the target directory is clean
rm -rf "$TMP_DIR"
rm -f "$ZIP_TARGET"
mkdir -p "$TMP_DIR"

# Copy source files (excluding test files and __pycache__)
for f in src/*; do
    basename=$(basename "$f")
    case "$basename" in
        test_*|__pycache__|.*)
            continue
            ;;
    esac
    cp -r "$f" "$TMP_DIR/"
done

# Export requirements and install dependencies
uv export --no-dev --format=requirements-txt > "$TARGET_DIR/requirements.txt"
uv pip install \
    -r "$TARGET_DIR/requirements.txt" \
    --target "$TMP_DIR" \
    --python-version "$PY_VERSION"

# Create ZIP file
pushd "$TMP_DIR"
zip -r "../$ZIP_NAME" .
popd

# Clean up temporary directory
rm -rf "$TMP_DIR"

# Output the ZIP path for GitHub Actions
echo "zip_path=$ZIP_TARGET" >> "$GITHUB_OUTPUT"
echo "Created ZIP: $ZIP_TARGET"

# Show ZIP contents for verification
echo "ZIP contents:"
unzip -l "$ZIP_TARGET" | head -20
