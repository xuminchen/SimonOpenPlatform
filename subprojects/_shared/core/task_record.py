from __future__ import annotations

import datetime
import inspect
import logging
import sys
import time
import uuid
from io import StringIO
from typing import Any, Callable

from subprojects._shared.core.db_client import connect_to_db


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def record_task(task_name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            task_id = str(uuid.uuid4())
            start_time = datetime.datetime.now()
            start_timestamp = time.time()
            status = "success"
            error_reason = None
            log_messages = []
            file_path = inspect.getfile(func)

            def log_message(message: str) -> None:
                logging.info(message)
                log_messages.append(message)

            log_message(f"File Path: {file_path}")
            log_message(f"Task {task_name} started.")

            old_stdout = sys.stdout
            new_stdout = StringIO()
            result = None
            try:
                sys.stdout = new_stdout
                result = func(*args, **kwargs)
            except Exception as exc:
                status = "failed"
                error_reason = str(exc)
                raise
            finally:
                print_output = new_stdout.getvalue()
                indented_output = "\n".join(["    " + line for line in print_output.splitlines()])
                if indented_output:
                    log_messages.append(indented_output)
                    print(indented_output)

                end_time = datetime.datetime.now()
                execution_time = time.time() - start_timestamp
                if status == "success":
                    log_message(f"Task {task_name} completed successfully in {execution_time:.2f} seconds.")
                else:
                    log_message(f"Task {task_name} failed in {execution_time:.2f} seconds: {error_reason}")
                log_text = "\n".join(log_messages)

                try:
                    conn = connect_to_db()
                    with conn.cursor() as cursor:
                        sql = (
                            "INSERT INTO task_records "
                            "(task_id, task_name, start_time, end_time, status, error_reason, log, execution_time, file_path) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        )
                        val = (
                            task_id,
                            task_name,
                            start_time,
                            end_time,
                            status,
                            error_reason,
                            log_text,
                            int(execution_time),
                            file_path,
                        )
                        cursor.execute(sql, val)
                        conn.commit()
                    conn.close()
                except Exception as exc:
                    logging.error(f"Database insertion error: {exc}")
                sys.stdout = old_stdout
            return result

        return wrapper

    return decorator
