import os

data_dir = os.environ['LVFS_UNITTESTDATA_DIR'] if 'LVFS_UNITTESTDATA_DIR' in os.environ \
    else os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data'))
