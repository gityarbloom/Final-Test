from elasticsearch import Elasticsearch
from datetime import datetime



es = Elasticsearch(['http://localhost:9200'])


def log_event(level, message, extra_info=None):
    document = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": level.upper(),
        "message": message
    }
    if extra_info:
        document.update(extra_info)

    try:
        es.index(index="intel-logs", document=document)
    except Exception as e:
        print(f"⚠️  [LOCAL LOG - {level.upper()}] {message} | Connection Error: {e}")