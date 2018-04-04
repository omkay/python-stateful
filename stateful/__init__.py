from sqlalchemy import MetaData, Table, Column, String, TIMESTAMP, create_engine
from sqlalchemy.sql import text
import logging
import time
import sys
reload(sys)  
sys.setdefaultencoding('utf-8')
from itertools import groupby
import namutil

import signal
class GracefulInterruptHandler(object):
    def __init__(self, sig=signal.SIGINT, interrupt_fn=None):
        self.sig = sig
        self.interrupt_fn = interrupt_fn

    def __enter__(self):
        self.interrupted = False
        self.released = False

        self.original_handler = signal.getsignal(self.sig)
        def handler(signum, frame):
            self.release()
            self.interrupted = True

        signal.signal(self.sig, handler)
        return self

    def __exit__(self, type, value, tb):
        self.release()

    def safe_interrupt(self):
        if self.interrupted and callable(self.interrupt_fn):
            self.interrupt_fn()

    def release(self):
        if self.released: return False
        signal.signal(self.sig, self.original_handler)
        self.released = True
        return True

class Stateful(object):
    def __init__(self, engine, work_fn, table="state", logger=None, sleep=0, work_kwargs=None, group_fn=None, task_key_fn=None):
        self.logger = logger or logging.getLogger('stateful')
        self.sleep = sleep
        self.work_kwargs = work_kwargs or {}
        self.work_fn = work_fn
        self.group_fn = group_fn
        self.task_key_fn = task_key_fn or (lambda task: (task[0] if isinstance(task, tuple) else task))

        self.engine = create_engine(engine)
        self.metadata = MetaData()
        self.table = Table(table, self.metadata, Column('key', String(50), primary_key=True), Column('t', TIMESTAMP, server_default=text('CURRENT_TIMESTAMP')))
        self.metadata.create_all(self.engine)

    def finish(self, key):
        self.engine.execute(self.table.insert().values(key=key))

    def filter_tasks(self, tasks_list):
        tasks_list = list(tasks_list)
        task_keys = [self.task_key_fn(task) for task in tasks_list]
        finished_tasks = set()
        for group in namutil.grouper(100, task_keys):
            finished_tasks.update([r['key'] for r in namutil.get_results_as_dict(self.engine, "SELECT `key` FROM `{table}` WHERE `key` IN :key_list", table=self.table.name, key_list=group)])
        return [task for task in tasks_list if self.task_key_fn(task) not in finished_tasks]

    def work(self, tasks_list):
        tasks = self.filter_tasks(tasks_list)
        fn = self.worker_group if callable(self.group_fn) else self.worker_nogroup
        return fn(tasks)

    def work_one(self, task):
        self.logger.info("Working on {}".format(task))
        self.work_fn(task=task, **self.work_kwargs)
        self.finish(self.task_key_fn(task))

    def worker_nogroup(self, tasks):
        errors = []
        with GracefulInterruptHandler(signal.SIGTERM, lambda: sys.exit(143)) as ih:
            for i, task in enumerate(tasks):
                ih.safe_interrupt()
                try:
                    if i != 0 and self.sleep: time.sleep(self.sleep)
                    self.logger.info("Working on {}".format(task))
                    self.work_fn(task=task, **self.work_kwargs)
                    self.finish(self.task_key_fn(task))
                except Exception:
                    self.logger.warn("Error while processing {}".format(task), exc_info=1)
                    errors.append(task)
                    ih.safe_interrupt()
                    time.sleep(self.sleep*2 or 10)
        return errors

    def worker_group(self, tasks):
        tasks.sort(key=self.group_fn)
        errors = []
        with GracefulInterruptHandler(signal.SIGTERM, lambda: sys.exit(143)) as ih:
            for i, (key, group) in enumerate(groupby(tasks, key=self.group_fn)):
                ih.safe_interrupt()
                try:
                    if i != 0 and self.sleep: time.sleep(self.sleep)
                    group = list(group)
                    self.logger.info("Working on {}: {}".format(key, group))
                    self.work_fn(key=key, tasks=group, **self.work_kwargs)
                    for task in group:
                        self.finish(self.task_key_fn(task))
                except Exception:
                    self.logger.warn("Error while processing {}: {}".format(key, group), exc_info=1)
                    errors.append(task)
                    ih.safe_interrupt()
                    time.sleep(self.sleep*2 or 10)
        return errors

