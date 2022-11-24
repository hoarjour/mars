from .... import oscar as mo
from ...core import AbstractService
from ..task_info_collector import TaskInfoCollectorActor


class TaskWorkerService(AbstractService):

    async def start(self):
        await mo.create_actor(
                TaskInfoCollectorActor, uid=TaskInfoCollectorActor.default_uid(), address=self._address
            )

    async def stop(self):
        await mo.destroy_actor(
            mo.create_actor_ref(uid=TaskInfoCollectorActor.default_uid(), address=self._address)
        )
