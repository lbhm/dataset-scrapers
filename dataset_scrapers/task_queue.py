import queue
from threading import Thread
from typing import Callable, Any


class TaskQueue(queue.Queue[tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any]]]):
    def __init__(self, num_workers: int = 1) -> None:
        queue.Queue.__init__(self)
        self.num_workers = num_workers
        self.start_workers()

    def add_task(self, task: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        args = args or ()
        kwargs = kwargs or {}
        self.put((task, args, kwargs))

    def start_workers(self) -> None:
        for _ in range(self.num_workers):
            t = Thread(target=self.worker)
            t.daemon = True
            t.start()

    def worker(self) -> None:
        while True:
            item, args, kwargs = self.get()
            item(*args, **kwargs)
            self.task_done()
