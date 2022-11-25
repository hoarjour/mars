# Copyright 1999-2021 Alibaba Group Holding Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import List, Union, Dict

from .... import oscar as mo
from ....core import Tileable
from ....lib.aio import alru_cache
from ...subtask import SubtaskResult
from ..core import TileableGraph, TaskResult
from ..supervisor.manager import TaskManagerActor
from ..task_info_collector import TaskInfoCollectorActor
from .core import AbstractTaskAPI


class TaskAPI(AbstractTaskAPI):
    def __init__(
            self,
            session_id: str,
            address: str
    ):
        self._session_id = session_id
        self._address = address

    @classmethod
    @alru_cache(cache_exceptions=False)
    async def create(cls, session_id: str, address: str) -> "TaskAPI":
        """
        Create Task API.

        Parameters
        ----------
        session_id : str
            Session ID
        address : str
            Supervisor address.

        Returns
        -------
        task_api
            Task API.
        """
        return TaskAPI(session_id, address)

    @alru_cache(cache_exceptions=False)
    async def _get_task_manager_ref(self):
        return await mo.actor_ref(
            self._address, TaskManagerActor.gen_uid(self._session_id)
        )

    @alru_cache(cache_exceptions=False)
    async def _get_task_info_collector_ref(self):
        return await mo.actor_ref(
            TaskInfoCollectorActor.default_uid(), address=self._address
        )

    async def get_task_results(self, progress: bool = False) -> List[TaskResult]:
        task_manager_ref = await self._get_task_manager_ref()
        return await task_manager_ref.get_task_results(progress)

    async def submit_tileable_graph(
        self,
        graph: TileableGraph,
        fuse_enabled: bool = None,
        extra_config: dict = None,
    ) -> str:
        try:
            task_manager_ref = await self._get_task_manager_ref()
            return await task_manager_ref.submit_tileable_graph(
                graph, fuse_enabled=fuse_enabled, extra_config=extra_config
            )
        except mo.ActorNotExist:
            raise RuntimeError("Session closed already")

    async def get_tileable_graph_as_json(self, task_id: str):
        task_manager_ref = await self._get_task_manager_ref()
        return await task_manager_ref.get_tileable_graph_dict_by_task_id(task_id)

    async def get_tileable_details(self, task_id: str):
        task_manager_ref = await self._get_task_manager_ref()
        return await task_manager_ref.get_tileable_details(task_id)

    async def get_tileable_subtasks(
        self, task_id: str, tileable_id: str, with_input_output: bool
    ):
        task_manager_ref = await self._get_task_manager_ref()
        return await task_manager_ref.get_tileable_subtasks(
            task_id, tileable_id, with_input_output
        )

    async def wait_task(self, task_id: str, timeout: float = None):
        task_manager_ref = await self._get_task_manager_ref()
        return await task_manager_ref.wait_task(task_id, timeout=timeout)

    async def get_task_result(self, task_id: str) -> TaskResult:
        task_manager_ref = await self._get_task_manager_ref()
        return await task_manager_ref.get_task_result(task_id)

    async def get_task_progress(self, task_id: str) -> float:
        task_manager_ref = await self._get_task_manager_ref()
        return await task_manager_ref.get_task_progress(task_id)

    async def cancel_task(self, task_id: str):
        task_manager_ref = await self._get_task_manager_ref()
        return await task_manager_ref.cancel_task(task_id)

    async def get_fetch_tileables(self, task_id: str) -> List[Tileable]:
        task_manager_ref = await self._get_task_manager_ref()
        return await task_manager_ref.get_task_result_tileables(task_id)

    async def set_subtask_result(self, subtask_result: SubtaskResult):
        task_manager_ref = await self._get_task_manager_ref()
        return await task_manager_ref.set_subtask_result.tell(subtask_result)

    async def get_last_idle_time(self) -> Union[float, None]:
        task_manager_ref = await self._get_task_manager_ref()
        return await task_manager_ref.get_last_idle_time()

    async def remove_tileables(self, tileable_keys: List[str]):
        task_manager_ref = await self._get_task_manager_ref()
        return await task_manager_ref.remove_tileables(tileable_keys)

    async def save_task_info(self, task_info: Dict, path: str):
        task_info_collector_ref = await self._get_task_info_collector_ref()
        await task_info_collector_ref.save_task_info(task_info, path)
