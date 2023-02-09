import os

from celery import Celery, signals
from app.celery.analyse import Analyse
import sentry_sdk
from sentry_sdk.integrations.celery import CeleryIntegration

# Initialize Celery
celery = Celery(
    'worker', 
    broker= os.environ.get("CELERY_BROKER_URL"),
    backend=os.environ.get("CELERY_RESULT_BACKEND")
)

@signals.celeryd_init.connect
def init_sentry(**_kwargs):
    if not os.environ["DEBUG"]:
        sentry_sdk.init(
            dsn="https://925f26cce4d34132bb9fcc5e38db1eed@o4504585220980736.ingest.sentry.io/4504649955278848",
            traces_sample_rate=1.0,
            integrations=[
                CeleryIntegration(),
            ]
        )

@celery.task()
def analyse_task(area: str, area_type: str):
    area = area.upper()
    area_type = area_type.upper()
    aggregator = Analyse()
    aggregator.run(area, area_type)
    del aggregator
    return area + area_type