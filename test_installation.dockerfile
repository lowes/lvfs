FROM python:3.7 AS starter
RUN useradd -d /home/nemo -m nemo
WORKDIR /home/nemo/lvfs
RUN pip install flit
COPY lvfs /home/nemo/lvfs/lvfs
COPY tests /home/nemo/lvfs/tests
COPY README.md pyproject.toml /home/nemo/lvfs/
RUN chown -R nemo /home/nemo
USER nemo
# Normally you don't override PATH because your normally don't use virtualenv inside docker
# But this is not a normal script.
ENV PATH /home/nemo/.local/bin:/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin


FROM starter AS wheel-build
RUN python3 -m venv .venv \
    && . .venv/bin/activate \
    && flit build

FROM starter AS symlink-install
RUN python3 -m venv .venv \
    && . .venv/bin/activate \
    && flit install -s

FROM starter AS normal-install
RUN python3 -m venv .venv \
    && . .venv/bin/activate \
    && flit install

FROM starter as novenv-install
RUN flit install

