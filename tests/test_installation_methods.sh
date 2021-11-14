#!/bin/sh
set -e

# First, build a wheel
(
    python3 -m venv .venv \
    && . .venv/bin/activate \
    && flit build \
    && py.test
)

# Next, try a symlink installation
(
    python3 -m venv .venv \
    && . .venv/bin/activate \
    && flit install -s \
    && py.test
)

# Now try a normal installation
(
    python3 -m venv .venv \
    && . .venv/bin/activate \
    && flit install \
    && py.test
)

# Last, try to install without a virtual environment
(
    flit install \
    && py.test
)