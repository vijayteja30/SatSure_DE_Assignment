from functools import wraps
from datetime import datetime
import logging

def record_task_timing(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logging.info(f"Context arguments for: {func} are: {args} \n\n {kwargs}")
        ti = kwargs.get('ti', None)
        
        logging.info(f"Task instance data is: {ti}")

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
