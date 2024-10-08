#!/usr/bin/env bash

set -euo pipefail
python3 --version
pip install --user -r requirements.txt

echo "Generating native binaries..."
rustup target add x86_64-pc-windows-msvc
# TODO: enable all features
cargo build --release --target x86_64-pc-windows-msvc --features lmdb

echo "Generating nostr_sdk.py..."
cd ../
cargo run -p uniffi-bindgen generate --library ../../target/x86_64-pc-windows-msvc/release/nostr_sdk_ffi.dll --language python --no-format -o bindings-python/src/nostr-sdk/

echo "Copying libraries nostr_sdk_ffi.dll..."
cp ../../target/x86_64-pc-windows-msvc/release/nostr_sdk_ffi.dll bindings-python/src/nostr-sdk/

echo "All done!"
