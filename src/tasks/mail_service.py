from celery_app import celery_app
from helpers.config import get_settings
import  logging
import asyncio
from datetime import datetime

from sqlalchemy.orm import defer

logger = logging.getLogger('celery.task')

@celery_app.task(bind=True)
def send_email_reports(self, mail_wait_seconds: int):

    return asyncio.run(_send_email_reports(self, mail_wait_seconds))

async def _send_email_reports(task_instance, mail_wait_seconds: int):

    started_at = str(datetime.now())
    task_instance.update_state(
        state='PROGRESS',
        meta={
            "started_at": started_at
        }
    )

    for ix in range(15):
        logger.info(f"Sending report {ix}")
        await asyncio.sleep(mail_wait_seconds)

    return {
        "no_emails": 15,
        "end_at": str(datetime.now())
    }