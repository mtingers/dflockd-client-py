# Installation

## Requirements

- Python 3.12 or later
- A running [dflockd](https://github.com/mtingers/dflockd) server
- No external Python dependencies

## Install from PyPI

=== "pip"

    ```bash
    pip install dflockd-client
    ```

=== "uv"

    ```bash
    uv add dflockd-client
    ```

## Install from source

```bash
git clone https://github.com/mtingers/dflockd-client-py.git
cd dflockd-client-py
uv sync
```

## Verify installation

```python
from dflockd_client import __version__
print(__version__)
```
