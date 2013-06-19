import sqlalchemy, logging
from time import sleep
import inspect, sys

def work_generator(fn):
    while True:
        kwargs = yield
        fn(**kwargs)


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
    def __init__(self, engine, work_fn, table="state", logger=None, sleep=0, work_kwargs=None):
        self.engine = sqlalchemy.create_engine(engine)
        self.table = table
        self.logger = logger or logging.getLogger('stateful')
        self.sleep = sleep
        self.work_kwargs = work_kwargs or {}
        self.work_fn = work_fn
        self.engine.execute("CREATE TABLE IF NOT EXISTS %s (id VARCHAR(50) PRIMARY KEY, t TIMESTAMP DEFAULT CURRENT_TIMESTAMP)" % self.table)

    def finish(self, id):
        self.engine.execute('insert into {} (id) values (%s)'.format(self.table), id)

    def get_tasks(self):
        return set([e[0] for e in self.engine.execute("select id from %s" % self.table)])

    def get_work_generator(self):
        if inspect.isgeneratorfunction(self.work_fn):
            gen = self.work_fn()
        else: gen = work_generator(self.work_fn)
        gen.send(None)
        return gen

    def work(self, tasks_list):
        tasks = dict(((task[0] if isinstance(task, tuple) else task), task) for task in tasks_list)
        new_tasks = set(tasks.keys()) - self.get_tasks()

        gen = self.get_work_generator()
        with GracefulInterruptHandler(signal.SIGTERM, lambda: sys.exit(143)) as ih:
            for i, task in enumerate(new_tasks):
                ih.safe_interrupt()
                try:
                    if i != 0 and self.sleep: sleep(self.sleep)
                    self.logger.info("Working on {}".format(task))
                    gen.send(dict(task=tasks[task], **self.work_kwargs))
                    self.finish(task)
                except Exception:
                    self.logger.warn("Error while processing {}".format(task), exc_info=1)
                    gen = self.get_work_generator()
                    ih.safe_interrupt()
                    sleep(self.sleep*2 or 10)

