# Set up
```
poetry install
```

## Run

### Development
```
poetry shell
export FLASK_APP=api/__init__.py && export FLASK_ENV=development && flask run --host 0.0.0.0
```

### Production
```
poetry run waitress-serve --port 5000 api:app &> /dev/null &
```

### Docker (WIP)
```
docker-compose down && docker-compose up -d
```
