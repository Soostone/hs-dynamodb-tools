#!/bin/bash
set -e

cabal sandbox init
cabal sandbox add-source deps/multi-compression
cabal sandbox add-source deps/katip/katip
cabal sandbox add-source deps/aws-utils
