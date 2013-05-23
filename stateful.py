import sqlalchemy, logging
from time import sleep
import traceback
import inspect

logger = logging.getLogger('stateful')

def work_generator(fn):
    while True:
        args = yield
        fn(*args)


class Stateful(object):
    def __init__(self, engine, work_fn, work_args=[], table="state", sleep=0):
        self.engine = sqlalchemy.create_engine(engine)
        self.table = table
        self.sleep = sleep
        self.work_fn, self.work_args = work_fn, work_args
        self.engine.execute("CREATE TABLE IF NOT EXISTS %s (id VARCHAR(50) PRIMARY KEY, t TIMESTAMP DEFAULT CURRENT_TIMESTAMP)" % self.table)

    def finish(self, id):
        self.engine.execute('insert into {} (id) values (%s)'.format(self.table), id)

    def get_tasks(self):
        return set([e[0] for e in self.engine.execute("select id from %s" % self.table)])

    def work(self, tasks_list):
        tasks = dict(((task[0] if isinstance(task, tuple) else task), task) for task in tasks_list)
        new_tasks = set(tasks.keys()) - self.get_tasks()

        def new_gen():
            if inspect.isgeneratorfunction(self.work_fn):
                gen = self.work_fn()
            else: gen = work_generator(self.work_fn)
            gen.send(None)
            return gen
        gen = new_gen()

        for i, task in enumerate(new_tasks):
            try:
                if self.sleep:
                    logger.info("Sleeping {} seconds before next task".format(self.sleep))
                    sleep(self.sleep)
                logger.info("Working on %s" % str(task))
                gen.send([tasks[task]] + list(self.work_args))
                self.finish(task)
            except Exception, e:
                traceback.print_exc()
                logger.warn("%s: %s" % (str(task), e))
                gen = new_gen()
                sleep(10)

