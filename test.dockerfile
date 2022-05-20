FROM python:3.9
RUN mkdir -p /app/lvfs \
&&  pip install -U pip \
&&  pip install flit \
&&  wget "https://dl.min.io/server/minio/release/linux-amd64/minio" \
&&  chmod 755 minio \
&&  mv minio /usr/bin/minio
RUN apt-get update \
&&  apt-get install -y openjdk-11-jre-headless \
&&  wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz \
&&  tar -zxf hadoop-3.3.1.tar.gz \
&&  rm hadoop-3.3.1.tar.gz \
&&  mv hadoop-3.3.1 /opt
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/
WORKDIR /app
COPY lvfs/__init__.py /app/lvfs/__init__.py
COPY README.md pyproject.toml /app/
RUN FLIT_ROOT_INSTALL=1 flit install -s

COPY lvfs /app/lvfs
COPY tests /app/tests
