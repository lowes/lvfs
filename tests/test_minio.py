from pathlib import Path
import subprocess
from lvfs import URL
from lvfs.credentials import Credentials
import pytest
import time
import logging
import os
import secrets

MINHOME = URL.to("s3://localhost:9000/default")

MINIO_PROCESSES = []
async def ensure_minio_is_running():
    if not MINIO_PROCESSES:
        # Populate test credentials
        access_key = secrets.token_hex(16)
        secret_key = secrets.token_hex(16)
        Credentials.register(dict(access_key=access_key, secret_key=secret_key), "Minio")
        
        logging.info("Starting Minio server")
        proc = subprocess.Popen(
            ["/usr/bin/minio", "server", Path.cwd().joinpath("tests/data").as_posix()],
            env=dict(MINIO_ROOT_USER=access_key, MINIO_ROOT_PASSWORD=secret_key, **os.environ)
        )
        try:
            proc.wait(3)
        except subprocess.TimeoutExpired:
            # Good! It's still running
            MINIO_PROCESSES.append(proc)
            await MINHOME.make_bucket()
        else:
            raise RuntimeError("Minio failed to run")

@pytest.mark.asyncio
async def test_can_start_minio():
    await ensure_minio_is_running()

@pytest.mark.asyncio
@pytest.mark.xfail
async def test_create_duplicate_bucket():
    # This already creates the default bucket
    await ensure_minio_is_running()
    # This checks that creating it again fails
    await MINHOME.make_bucket()

@pytest.mark.asyncio
async def test_minio_json():
    await ensure_minio_is_running()
    basicjson = MINHOME.join("example.json")
    await basicjson.write_json({"key": "value"})
    assert (await basicjson.read_json()) == {"key": "value"}