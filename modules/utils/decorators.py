from functools import wraps
from datetime import datetime
import logging

def record_task_timing(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        context = kwargs.get("context") or (args[0] if args else None)
        ti = context.get("ti") if context else None

        start_time = datetime.utcnow()
        if ti:
            ti.xcom_push(key="start_time", value=start_time.isoformat())

        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Task '{func.__name__}' failed: {str(e)}")
            raise
        finally:
            end_time = datetime.utcnow()
            if ti:
                ti.xcom_push(key="end_time", value=end_time.isoformat())
    return wrapper
