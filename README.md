# Gen3 Analysis API

## Overview

The Gen3 Analysis service allows analysis queries

## Details

This repo is a standard CRUD REST API. This service is
built on the fastapi framework.

- Use `run.sh` to spin up a `localhost` instance of the API
- Use `test.sh` to run all the tests
- Use `clean.sh` to run several formatting and linting
  commands

We use `.env` files to hold all configurations for different
environment configurations. More information about accepted
configurations can be found under the docs folder in the
example `env` file


## Quickstart

### Setup

The API should nearly work out of the box. You will
need to install poetry dependencies, as well as set
up a `.env` file at the top level. Once you have
a `.env` set up, running `run.sh` should boot up
an API you can access in your browser by going to
`localhost:8000` assuming you use the default ports.

#### Configuration

The configuration is done via a `.env` which allows environment variable overrides if you don't want to use the actual
file.
