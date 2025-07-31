"""
This is a single Python entry-point for a simple run, intended to be used
for debugging.

In general, you should prefer the `run.sh` and `test.sh` scripts in this
directory for running the service and testing. But if you need to debug
the running service (from PyCharm, for example), this is a good
script to use (if you properly setup everything else external to this).

Specifically, this assumes you have properly migrated the database and have the needed
environment variables for prometheus (and any other setup done by the
bash scripts in this same directory).

Usage:
- Run app: python run.py
- Generate openapi docs: python run.py openapi
"""

import os
import sys

import uvicorn
import yaml

from gen3analysis.main import get_app


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


if __name__ == "__main__":
    if sys.argv[-1] == "openapi":  # generate openapi docs
        app = get_app()
        schema = app.openapi()
        path = os.path.join(CURRENT_DIR, "docs/openapi.yaml")
        yaml.Dumper.ignore_aliases = lambda *args: True
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w+") as f:
            yaml.dump(schema, f, default_flow_style=False)
        print(f"Saved docs at {path}")
    else:
        host = "0.0.0.0"
        port = 8000
        print(f"gen3analysis.main:app_instance running at {host}:{port}")
        uvicorn.run("gen3analysis.main:app_instance", host=host, port=port, reload=True)
