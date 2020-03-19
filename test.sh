#!/usr/bin/env bash
set -euxo pipefail
export PATH=$PATH:${HOME}/.cargo/bin
cargo build --all
pushd jsonrpc && cargo test && popd
pushd mayastor && cargo test -- --test-threads=1 && popd
pushd mayastor-test && npm install && ./node_modules/mocha/bin/mocha test_cli.js && popd
pushd mayastor-test && ./node_modules/mocha/bin/mocha test_replica.js && popd
pushd mayastor-test && ./node_modules/mocha/bin/mocha test_csi.js && popd
pushd mayastor-test && ./node_modules/mocha/bin/mocha test_nexus.js && popd
pushd mayastor-test && ./node_modules/mocha/bin/mocha test_rebuild.js && popd
