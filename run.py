import sys
import app.flow.commons as commons
from time import time
import os

def run():
    from app.flow import (
        send,
    )
    task_list = [
        send.DynamicSend(**commons.params),
    ]

    return task_list


if __name__ == '__main__':

    commons.logger.info(f'Running')
    commons.logger.debug(commons.params)
    task_list = run()

    t0 = time()
    for task in task_list:
        commons.logger.debug(f'Starting task: "{task}')
        exec_status = commons.luigi.build([task], local_scheduler=True,
                                          workers=int(os.environ.get("WORKERS", 4)),
                                          scheduler_port=8880,
                                          detailed_summary=True)

        commons.logger.debug(f'Finished {task}, Elapsed time: {time() - t0}')
        if exec_status.status.name == 'SUCCESS_WITH_RETRY' or exec_status.status.name == 'SUCCESS':
            continue
        else:
            commons.logger.error(f'Error: Elapsed time: {time() - t0}')
            sys.exit(1)

    commons.logger.info(f'Elapsed time: {time() - t0}')