# eodhp-ades-stageout

### Repo to containerise the stageout script used in the EODHP Workflow Runner

The code in this repo is based on that included in the EOEPCA project. It handles the resulting STAC Catalog output by the ADES workflow runner and uploads the STAC catalog to an S3 bucket.

# Development of this component

## Getting started

You will need Python 3.13 and [uv](https://docs.astral.sh/uv/getting-started/installation/).

### Install via makefile

```commandline
make setup
```

This will install all dependencies using `uv sync` and set up `pre-commit` hooks.

It's safe and fast to run `make setup` repeatedly as it will only update things if
they have changed.

After `make setup` you can run `make pre-commit` to run pre-commit checks on staged changes and
`make pre-commit-all` to run them on all files.


## Building and testing

This component uses `pytest` tests and `ruff` for linting and formatting, and `pyright` for type checking.

A number of `make` targets are defined:
* `make test`: run tests continuously (watch mode)
* `make testonce`: run tests once
* `make check`: run all linting, formatting checks, type checking and pyproject validation
* `make format`: auto-fix linting issues and reformat code
* `make dockerbuild`: build a `latest` Docker image (use `make dockerbuild VERSION=1.2.3` for a release image)
* `make dockerpush`: push a Docker image (add `VERSION=1.2.3` for a versioned release)

## Managing requirements

Dependencies are specified in `pyproject.toml`. After changing them, run:

```commandline
uv sync
```

This will update the `uv.lock` file. Commit the updated `uv.lock` along with `pyproject.toml`.

## Releasing

Ensure that `make check` and `make testonce` pass with no errors before continuing.

Releases tagged `latest` and targeted at development environments can be created from the `main` branch. Releases for
installation in non-development environments should be created from a Git tag named using semantic versioning. For
example, using

* `git tag v1.2.3`
* `git push --tags`

Docker images will be built automatically by GitHub Actions after pushing to the EO-DataHub repos.
