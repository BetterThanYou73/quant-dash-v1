web: uvicorn backend.main:app --host 0.0.0.0 --port $PORT --workers 1
release: python -m core.workers --once --task=daily
