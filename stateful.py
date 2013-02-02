import sqlalchemy, logging
from time import sleep
import traceback

logger = logging.getLogger('stateful')

class Stateful(object):
    def __init__(self, engine, work_fn, work_args=[], work_kwargs={}, table="state"):
        self.engine = sqlalchemy.create_engine(engine)
        self.table = table
        self.work_fn, self.work_args, self.work_kwargs = work_fn, work_args, work_kwargs
        self.engine.execute("CREATE TABLE IF NOT EXISTS %s (id VARCHAR(50) PRIMARY KEY, t TIMESTAMP DEFAULT CURRENT_TIMESTAMP)" % self.table)

    def finish(self, id):
        self.engine.execute('insert into %s  ("id") values (?)' % self.table, id)

    def get_tasks(self):
        return set([e[0] for e in self.engine.execute("select id from %s" % self.table)])


    def work(self, tasks):
        new_tasks = set(tasks) - self.get_tasks()
        for task in new_tasks:
            try: 
                logger.info("Working on %s" % task)
                self.work_fn(task, *self.work_args, **self.work_kwargs)
                self.finish(task)
            except Exception, e:
                traceback.print_exc()
                logger.warn("%s: %s" % (task, e))
                sleep(10)


