import subprocess
from lvfs import URL
from lvfs.credentials import Credentials
import logging
import pytest

HDFSHOME = URL.to("hdfs://localhost/default")

HADOOP_PROCESSES = []
async def ensure_hadoop_is_running():
    if not HADOOP_PROCESSES:
        # Populate test credentials
        Credentials.register(dict(username="root", webhdfs_root="http://localhost:50070/"), "HDFSOverSSH")
        
        logging.info("Starting Hadoop minicluster")
        proc = subprocess.Popen(
            ["/opt/hadoop-3.3.1/bin/mapred", "minicluster", "-nomr", "-format", "-nnhttpport", "50070", "-D", "dfs.webhdfs.enabled=true"],
            cwd="/opt/hadoop-3.3.1/"
        )
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            # Good! It's still running
            HADOOP_PROCESSES.append(proc)
            await HDFSHOME.make_bucket()
        else:
            raise RuntimeError("Hadoop Minicluster failed to run")

@pytest.mark.asyncio
async def test_can_start_hadoop():
    await ensure_hadoop_is_running()

@pytest.mark.asyncio
async def test_hdfs_json():
    await ensure_hadoop_is_running()
    basicjson = HDFSHOME.join("example.json")
    await basicjson.write_json({"key": "value"})
    assert (await basicjson.read_json()) == {"key": "value"}