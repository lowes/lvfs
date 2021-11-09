FROM python:3.9
RUN mkdir -p /app/lvfs \
&&  pip install -U pip \
&&  pip install flit \
&&  wget "https://dl.min.io/server/minio/release/linux-amd64/minio" \
&&  chmod 755 minio \
&&  mv minio /usr/bin/minio
WORKDIR /app
COPY lvfs/__init__.py /app/lvfs/__init__.py
COPY README.md pyproject.toml /app/
RUN FLIT_ROOT_INSTALL=1 flit install -s

COPY lvfs /app/lvfs
COPY tests /app/tests
COPY tests/data/default-lvfs.yml /root/.config/lvfs.yml