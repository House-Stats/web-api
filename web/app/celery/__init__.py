import os

from celery import Celery
from app.celery.analyse import Analyse

# Initialize Celery
celery = Celery(
    'worker', 
    broker= os.environ.get("CELERY_BROKER_URL", "pyamqp://User:pass@localhost:5672"),
    backend=os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost")
)

@celery.task()
def analyse_task(area: str, area_type: str):
    area = area.upper()
    area_type = area_type.upper()
    aggregator = Analyse()
    aggregator.run(area, area_type)
    return area + area_type