import os
import pandas as pd
import warnings
import asyncio
import pytest
from lvfs import URL
from tests import data_dir
from tempfile import TemporaryDirectory


@pytest.mark.asyncio
async def test_read_json():
    x = await URL.to(os.path.join(data_dir, "test_json.json")).read_json()
    assert x['best_animal'] == 'kittens'

@pytest.mark.asyncio
async def test_write_json():
    with TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "tmp.json")
        await URL.to(file_path).write_json({"second_best_animal": "puppies"})
        x = await URL.to(file_path).read_json()
    assert x['second_best_animal'] == 'puppies'

@pytest.mark.asyncio
async def test_read_yaml():
    x = await URL.to(os.path.join(data_dir, "test_yaml.yml")).read_yaml()
    assert all([v in ['do', 're', 'mi', 'fa'] for v in x.keys() if v != 'other notes'])
    assert sorted(x['other notes']) == ['do', 'la', 'so', 'ti']

@pytest.mark.asyncio
async def test_write_yaml():
    with TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "tmp.yml")
        await URL.to(file_path).write_yaml({"second_best_animal": "puppies"})
        x = await URL.to(file_path).read_yaml()
    assert x['second_best_animal'] == 'puppies'

@pytest.mark.asyncio
async def test_read_pickle():
    x = await URL.to(os.path.join(data_dir, "test_pkl.pkl")).read_pickle()
    assert len(x) == 6
    assert 'cucumbers' in x

@pytest.mark.asyncio
async def test_write_pickle():
    with TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "tmp.pkl")
        await URL.to(file_path).write_pickle({"where its at": "two turn tables"})
        x = await URL.to(file_path).read_pickle()
    assert x['where its at'] == 'two turn tables'

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

@pytest.mark.asyncio
async def test_write_parquet():
    warnings.simplefilter(action="ignore", category=DeprecationWarning)
    warnings.simplefilter(action="ignore", category=FutureWarning)
    with TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "tmp.pq")
        pd.DataFrame({'a': ['Gwen', 'once', 'exclaimed', 'that', 'this', 'is', 'bananas'],
                            'b': ['b', 'a', 'n', 'a', 'n', 'a', 's']})\
            .to_parquet(file_path)
        x = await URL.to(file_path).read_parquet()
    assert all(x['b'].values == ['b', 'a', 'n', 'a', 'n', 'a', 's'])

@pytest.mark.asyncio
async def test_ls():
    the_truth = "There are three kinds of lies: lies, damned lies, and statistics."
    with TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "tmp.txt")
        await URL.to(file_path).write_text(the_truth)
        x = list(await URL.to(tmpdir).ls())
    assert len(x) == 1
    assert x[0].path == file_path

@pytest.mark.asyncio
async def test_cp():
    the_truth = "There are three kinds of lies: lies, damned lies, and statistics."
    with TemporaryDirectory() as tmpdir:
        file_path_1 = os.path.join(tmpdir, "tmp_1.txt")
        file_path_2 = os.path.join(tmpdir, "tmp_2.txt")
        await URL.to(file_path_1).write_text(the_truth)
        await URL.to(file_path_1).cp(file_path_2)
        x = list(await URL.to(tmpdir).ls())
        t_1 = await URL.to(file_path_1).read_text()
        t_2 = await URL.to(file_path_2).read_text()
    assert len(x) == 2
    assert file_path_1 in x
    assert file_path_2 in x
    assert t_1 == t_2

@pytest.mark.asyncio
async def test_rm():
    the_truth = "There are three kinds of lies: lies, damned lies, and statistics."
    with TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "tmp.txt")
        await URL.to(file_path).write_text(the_truth)
        await URL.to(file_path).rm()
        x = list(await URL.to(tmpdir).ls())
    assert len(x) == 0

@pytest.mark.asyncio
async def test_mv():
    the_truth = "There are three kinds of lies: lies, damned lies, and statistics."
    with TemporaryDirectory() as tmpdir:
        file_path_1 = os.path.join(tmpdir, "tmp_1.txt")
        file_path_2 = os.path.join(tmpdir, "tmp_2.txt")
        await URL.to(file_path_1).write_text(the_truth)
        t_1 = await URL.to(file_path_1).read_text()
        await URL.to(file_path_1).mv(file_path_2)
        x = list(await URL.to(tmpdir).ls())
        t_2 = await URL.to(file_path_2).read_text()
    assert len(x) == 1
    assert file_path_1 not in x
    assert file_path_2 in x
    assert t_1 == t_2

@pytest.mark.asyncio
async def test_isdir():
    with TemporaryDirectory() as tmpdir:
        assert await URL.to(tmpdir).isdir()
        assert not await URL.to(f"{tmpdir}_this_shouldnt_exist").isdir()
