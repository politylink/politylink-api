```bash
poetry install
poetry shell
export FLASK_APP=api/__init__.py && export FLASK_ENV=development && flask run --host 0.0.0.0
```