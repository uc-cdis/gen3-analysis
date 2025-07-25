ARG AZLINUX_BASE_VERSION=master
FROM quay.io/cdis/python-nginx-al:${AZLINUX_BASE_VERSION} AS base

ENV appname=gen3analysis

WORKDIR /${appname}

RUN chown -R gen3:gen3 /${appname}

# Builder stage
FROM base AS builder

USER gen3

# copy ONLY poetry artifact, install the dependencies but not the app;
# this will make sure that the dependencies are cached
COPY poetry.lock pyproject.toml /${appname}/
RUN poetry install -vv --no-root --only main --no-interaction

COPY --chown=gen3:gen3 . /${appname}

# install the app
RUN poetry install --without dev --no-interaction

# Final stage
FROM base

COPY --from=builder /${appname} /${appname}

RUN dnf -y install vim

# Switch to non-root user 'gen3' for the serving process
USER gen3

WORKDIR /${appname}

RUN chmod 755 bin/run.sh

CMD ["bash", "bin/run.sh"]
