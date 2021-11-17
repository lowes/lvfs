import os
import warnings
import pytest
from lvfs import URL
from tests import data_dir
from tempfile import TemporaryDirectory


@pytest.mark.asyncio
async def test_read_json():
    x = await URL.to(os.path.join(data_dir, "test_json.json")).read_json()
    assert x['best_animal'] == 'kittens'

@pytest.mark.asyncio
async def test_read_yaml():
    x = await URL.to(os.path.join(data_dir, "test_yaml.yml")).read_yaml()
    assert all([v in ['do', 're', 'mi', 'fa'] for v in x.keys() if v != 'other notes'])
    assert sorted(x['other notes']) == ['do', 'la', 'so', 'ti']

@pytest.mark.asyncio
async def test_read_pickle():
    x = await URL.to(os.path.join(data_dir, "test_pkl.pkl")).read_pickle()
    assert len(x) == 6
    assert 'cucumbers' in x

@pytest.mark.asyncio
async def test_read_text():
    x = await URL.to(os.path.join(data_dir, "dummy.txt")).read_text()
    assert x == 'this file exists as a placeholder for the data directory.'

@pytest.mark.asyncio
async def test_write_text():
    the_truth = "There are three kinds of lies: lies, damned lies, and statistics."
    with TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "tmp.txt")
        await URL.to(file_path).write_text(the_truth)
        x = await URL.to(file_path).read_text()
    assert x == the_truth

@pytest.mark.asyncio
async def test_read_parquet():
    warnings.simplefilter(action="ignore", category=DeprecationWarning)
    warnings.simplefilter(action="ignore", category=FutureWarning)
    x = await URL.to(os.path.join(data_dir, "test_parquet.pq")).read_parquet()
    assert x.to_json() == '{"x":{"0":1,"1":2,"2":3},"y":{"0":4,"1":5,"2":6}}'
