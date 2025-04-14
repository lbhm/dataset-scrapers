import queue
from collections.abc import Callable
from threading import Thread
from typing import Any


class TaskQueue(queue.Queue[tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any]]]):
    def __init__(self, num_workers: int = 1, maxsize: int = 0) -> None:
        super().__init__(maxsize=maxsize)
        self.num_workers = num_workers
        self.threads: list[Thread] = []

        self.start_workers()

    def __del__(self) -> None:
        self.stop_workers()

    def add_task(self, task: Callable[..., Any], *args: Any, **kwargs: Any) -> None:  # noqa: ANN401
        self.put((task, args, kwargs))

    def start_workers(self) -> None:
        for _ in range(self.num_workers):
            t = Thread(target=self.worker, daemon=True)
            self.threads.append(t)
            t.start()

    def stop_workers(self) -> None:
        for thread in self.threads:
            del thread

    def worker(self) -> None:
        while True:
            item, args, kwargs = self.get()
            item(*args, **kwargs)
            self.task_done()
