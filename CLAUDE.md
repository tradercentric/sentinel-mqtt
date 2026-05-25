# sentinel-mqtt

## Environment

Always use a virtual environment. Never install packages with `--break-system-packages` or into the user/system Python.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Run tests:
```bash
source .venv/bin/activate
pytest tests/ -v
```

The `.venv/` directory is gitignored.
