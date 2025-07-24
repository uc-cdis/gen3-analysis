# Gen3 Analysis API

![version](https://img.shields.io/github/release/uc-cdis/gen3-analysis.svg) [![Apache license](http://img.shields.io/badge/license-Apache-blue.svg?style=flat)](LICENSE) [![Coverage Status](https://coveralls.io/repos/github/uc-cdis/gen3-analysis/badge.svg?branch=master)](https://coveralls.io/github/uc-cdis/gen3-analysis?branch=master)

## Overview

The Gen3 Analysis service allows users to run analysis queries on Gen3 data.

## Details

This repo is a standard CRUD REST API. This service is
built on the fastapi framework.

- Use `bin/run.sh` to spin up a `localhost` instance of the API
- Use `bin/test.sh` to run all the tests
- Use `bin/clean.sh` to run several formatting and linting
  commands

## Quickstart

### Setup

The API should nearly work out of the box. You will need to install poetry dependencies, as well as set up a `.env` file at the top level. Once you have a `.env` set up, running `bin/run.sh` should boot up an API you can access in your browser by going to `localhost:8000`, assuming you use the default ports.

#### Configuration

The configuration is done via a `.env` file. More information about accepted configurations can be found in `gen3analysis/config.py`.
