from lvfs import URL
from tests.test_minio import ensure_minio_is_running
import pytest
import pandas as pd

argnames = "home,init"
homes = [
    (URL.to("/tmp/lvfs-test"), lambda home: home.mkdir()),
    (URL.to("s3://localhost:9000/default"), lambda home: ensure_minio_is_running())
]
home_names = ["local", "minio"]

@pytest.mark.parametrize(argnames, homes, ids=home_names)
@pytest.mark.asyncio
async def test_write_dicts(home: URL, init):
    await init(home)
    path = home.join("tmp")
    for get, put in [
        (path.read_json, path.write_json),
        (path.read_yaml, path.write_yaml),
        (path.read_pickle, path.write_pickle)
    ]:
        await put({"second_best_animal": "puppies"})
        assert (await get())['second_best_animal'] == 'puppies'

@pytest.mark.parametrize(argnames, homes, ids=home_names)
@pytest.mark.asyncio
async def test_write_tables(home: URL, init):
    await init(home)
    path = home.join("tmp")
    df = pd.DataFrame({'a': ['Gwen', 'once', 'exclaimed', 'that', 'this', 'is', 'bananas'],
                       'b': ['b', 'a', 'n', 'a', 'n', 'a', 's']})
    for get, put in [
        (path.read_parquet, path.write_parquet),
        (path.read_orc, path.write_orc),
        (path.read_csv, path.write_csv)
    ]:
        await put(df)
        assert (await get())['b'].values.tolist() == list('bananas')

@pytest.mark.parametrize(argnames, homes, ids=home_names)
@pytest.mark.asyncio
async def test_cp(home: URL, init):
    await init(home)
    the_truth = "There are three kinds of lies: lies, damned lies, and statistics."
    home = home.join("test-cp")
    await home.mkdir()
    path_1 = home.join("tmp_1.txt")
    path_2 = home.join("tmp_2.txt")
    await path_1.write_text(the_truth)
    await path_1.cp(path_2)

    # These should be the only things in the directory
    filenames = await home.ls()
    assert len(filenames) == 2
    assert path_1 in filenames
    assert path_2 in filenames

    # File content should agree
    assert (await path_1.read_text()) == (await path_2.read_text())

@pytest.mark.parametrize(argnames, homes, ids=home_names)
@pytest.mark.asyncio
async def test_mv(home: URL, init):
    await init(home)
    the_truth = "There are three kinds of lies: lies, damned lies, and statistics."
    home = home.join("test-mv")
    await home.mkdir()
    path_1 = home.join("tmp_1.txt")
    path_2 = home.join("tmp_2.txt")
    # Writing makes the file
    assert not await path_1.exists()
    await path_1.write_text(the_truth)
    assert await path_1.exists()

    # Then moving creates a new file and deletes the old
    assert not await path_2.exists()
    await path_1.mv(path_2)
    assert not await path_1.exists()
    assert await path_2.exists()

    # The filenames should agree
    filenames = await home.ls()
    assert len(filenames) == 1
    assert path_1 not in filenames
    assert path_2 in filenames

    # And lastly the content should match
    assert the_truth == await path_2.read_text()

@pytest.mark.parametrize(argnames, homes, ids=home_names)
@pytest.mark.asyncio
async def test_directories(home: URL, init):
    await init(home)
    item = home.join("test-directory")

    # Should list correctly
    await item.write_text("nothing")
    assert item in await home.ls()

    # Now delete the item, and it should be gone
    await item.rm()
    assert item not in await home.ls()

    # Cool. Make a new directory
    if home.supports_directories():
        await item.mkdir()
        assert item in await home.ls()
        assert await item.isdir()

    # Even if the directory is not supported its content will be empty
    assert [] == await item.ls()

    # Make sure you can use the directory too
    await item.join("x").write_text("bogus")
    # And at this point it should always list content
    assert item.join("x") in await item.ls()

    # Should be able to recursively delete even if directories are not supported
    await item.rm()
    assert item not in await home.ls()
