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
import functools
import itertools
from io import StringIO
import os
from typing import Dict, List

from ....core.operand import Fetch, FetchShuffle
from ....lib.aio import alru_cache
from ....typing import ChunkType, TileableType
from ....utils import calc_data_size
from ...subtask import Subtask, SubtaskGraph
from ..core import Task


result_chunk_to_subtask = dict()


class GraphVisualizer:
    @classmethod
    def to_dot(cls, subtask_graphs: List[SubtaskGraph]):
        sio = StringIO()
        sio.write("digraph {\n")
        sio.write("splines=curved\n")
        sio.write("rankdir=BT\n")
        sio.write("graph [compound=true];\n")
        subgraph_index = 0
        current_stage = 0
        result_chunk_to_subtask = dict()
        line_colors = dict()
        color_iter = iter(itertools.cycle(range(1, 9)))
        for stage_line in itertools.combinations(range(len(subtask_graphs))[::-1], 2):
            line_colors[stage_line] = f'"/spectral9/{next(color_iter)}"'

        for subtask_graph in subtask_graphs:
            for subtask in subtask_graph.topological_iter():
                current_cluster = f"cluster_{subgraph_index}"
                sio.write(
                    cls._export_subtask_to_dot(
                        subtask,
                        current_cluster,
                        current_stage,
                        line_colors,
                        result_chunk_to_subtask,
                    )
                )
                for c in subtask.chunk_graph.results:
                    result_chunk_to_subtask[c.key] = [current_stage, current_cluster]
                subgraph_index += 1
            current_stage += 1
        sio.write("}")
        return sio.getvalue()

    @classmethod
    def _gen_chunk_key(cls, chunk, trunc_key):
        if "_" in chunk.key:
            key, index = chunk.key.split("_", 1)
            return "_".join([key[:trunc_key], index])
        else:  # pragma: no cover
            return chunk.key[:trunc_key]

    @classmethod
    def _export_subtask_to_dot(
        cls,
        subtask: Subtask,
        subgraph_name: str,
        current_stage: int,
        line_colors: Dict,
        chunk_key_to_subtask: Dict[str, List],
        trunc_key: int = 5,
    ):

        chunk_graph = subtask.chunk_graph
        sio = StringIO()
        chunk_style = "[shape=box]"
        operand_style = "[shape=circle]"

        visited = set()
        all_nodes = []
        for node in chunk_graph.iter_nodes():
            op = node.op
            if isinstance(node.op, (Fetch, FetchShuffle)):
                continue
            op_name = type(op).__name__
            if op.stage is not None:
                op_name = f"{op_name}:{op.stage.name}"
            if op.key in visited:
                continue
            for input_chunk in op.inputs or []:
                if input_chunk.key not in visited and not isinstance(
                    input_chunk.op, (Fetch, FetchShuffle)
                ):  # pragma: no cover
                    node_name = f'"Chunk:{cls._gen_chunk_key(input_chunk, trunc_key)}"'
                    sio.write(f"{node_name} {chunk_style}\n")
                    all_nodes.append(node_name)
                    visited.add(input_chunk.key)
                if op.key not in visited:
                    node_name = f'"{op_name}:{op.key[:trunc_key]}"'
                    sio.write(f"{node_name} {operand_style}\n")
                    all_nodes.append(node_name)
                    visited.add(op.key)
                if (
                    isinstance(input_chunk.op, (Fetch, FetchShuffle))
                    and input_chunk.key in chunk_key_to_subtask
                ):
                    stage, tail_cluster = chunk_key_to_subtask[input_chunk.key]
                    if stage == current_stage:
                        line_style = "style=bold"
                    else:
                        line_style = (
                            f"style=bold color={line_colors[(current_stage, stage)]}"
                        )
                    sio.write(
                        f'"Chunk:{cls._gen_chunk_key(input_chunk, trunc_key)}" ->'
                        f' "{op_name}:{op.key[:trunc_key]}" '
                        f"[lhead={subgraph_name} ltail={tail_cluster} {line_style}];\n"
                    )
                else:
                    sio.write(
                        f'"Chunk:{cls._gen_chunk_key(input_chunk, trunc_key)}" -> '
                        f'"{op_name}:{op.key[:trunc_key]}"\n'
                    )

            for output_chunk in op.outputs or []:
                if output_chunk.key not in visited:
                    node_name = f'"Chunk:{cls._gen_chunk_key(output_chunk, trunc_key)}"'
                    sio.write(f"{node_name} {chunk_style}\n")
                    all_nodes.append(node_name)
                    visited.add(output_chunk.key)
                if op.key not in visited:
                    node_name = f'"{op_name}:{op.key[:trunc_key]}"'
                    sio.write(f"{node_name} {operand_style}\n")
                    all_nodes.append(node_name)
                    visited.add(op.key)
                sio.write(
                    f'"{op_name}:{op.key[:trunc_key]}" -> '
                    f'"Chunk:{cls._gen_chunk_key(output_chunk, trunc_key)}"\n'
                )
        # write subgraph info
        sio.write(f"subgraph {subgraph_name} {{\n")
        nodes_str = " ".join(all_nodes)
        sio.write(f"{nodes_str};\n")
        sio.write(f'label="{subtask.subtask_id}";\n}}')
        sio.write("\n")
        return sio.getvalue()


def collect_verify(f):
    @functools.wraps(f)
    async def wrapper(*args, **kwargs):
        if hasattr(args[0], "collect_info") and args[0].collect_info:
            await f(*args, **kwargs)
        else:
            pass

    return wrapper


class YamlDumper:

    def __init__(self, address, collect_info=False):
        self.collect_info = collect_info

        self._address = address
        self._cluster_api = None

    @alru_cache(cache_exceptions=False)
    async def _get_cluster_api(self):
        from ...cluster import ClusterAPI

        return await ClusterAPI.create(self._address)

    @collect_verify
    async def collect_subtask_operand_structure(self,
                                                task: Task,
                                                subtask_graph: SubtaskGraph,
                                                stage_id: str,
                                                trunc_key: int = 5,
                                                ):
        session_id, task_id = task.session_id, task.task_id
        save_path = os.path.join(session_id, task_id)

        if task_id not in result_chunk_to_subtask:
            result_chunk_to_subtask[task_id] = dict()

        for subtask in subtask_graph.topological_iter():
            chunk_graph = subtask.chunk_graph
            for c in chunk_graph.results:
                result_chunk_to_subtask[task_id][c.key] = subtask.subtask_id

            visited = set()
            subtask_dict = dict()
            subtask_dict["pre_subtasks"] = list()
            subtask_dict["ops"] = list()
            subtask_dict["stage_id"] = stage_id

            for node in chunk_graph.iter_nodes():
                op = node.op
                if isinstance(op, (Fetch, FetchShuffle)):
                    continue
                if op.key in visited:
                    continue

                subtask_dict["ops"].append(op.key)
                op_dict = dict()
                op_name = type(op).__name__
                op_dict["op_name"] = op_name
                if op.stage is not None:
                    op_dict["stage"] = op.stage.name
                else:
                    op_dict["stage"] = None
                op_dict["subtask_id"] = subtask.subtask_id
                op_dict["stage_id"] = stage_id
                op_dict["predecessors"] = list()
                op_dict["inputs"] = dict()

                for input_chunk in op.inputs or []:
                    if input_chunk.key not in visited:
                        op_dict["predecessors"].append(input_chunk.key)
                        if (
                            isinstance(input_chunk.op, (Fetch, FetchShuffle))
                            and input_chunk.key in result_chunk_to_subtask[task_id]
                        ):
                            subtask_dict["pre_subtasks"].append(result_chunk_to_subtask[task_id][input_chunk.key])

                        chunk_dict = dict()
                        if hasattr(input_chunk, "dtypes") and input_chunk.dtypes is not None:
                            unique_dtypes = list(set(input_chunk.dtypes.map(str)))
                            chunk_dict["dtypes"] = unique_dtypes
                        elif hasattr(input_chunk, "dtype") and input_chunk.dtype is not None:
                            chunk_dict["dtype"] = str(input_chunk.dtype)

                        op_dict["inputs"][input_chunk.key] = chunk_dict

                op_save_path = os.path.join(save_path, f"{stage_id[:trunc_key]}_operand.yaml")
                await self.save_yaml({op.key: op_dict}, op_save_path)
                visited.add(op.key)

            subtask_save_path = os.path.join(save_path, f"{stage_id[:trunc_key]}_subtask.yaml")
            await self.save_yaml({subtask.subtask_id: subtask_dict}, subtask_save_path)

    @collect_verify
    async def collect_last_node_info(self, task: Task, subtask_graphs: List[SubtaskGraph]):
        last_op_keys = []
        last_subtask_keys = []
        subtask_graph = subtask_graphs[-1]
        for subtask in subtask_graph.topological_iter():
            if len(subtask_graph._successors[subtask]) == 0:
                last_subtask_keys.append(subtask.subtask_id)
                for res in subtask.chunk_graph.results:
                    last_op_keys.append(res.op.key)

        save_path = os.path.join(task.session_id, task.task_id, "last_nodes.yaml")
        await self.save_yaml({"op": last_op_keys,
                              "subtask": last_subtask_keys},
                             save_path)

    @collect_verify
    async def collect_tileable_structure(self,
                                         task: Task,
                                         tileable_to_subtasks: Dict[TileableType, List[Subtask]]):
        tileable_dict = dict()
        for tileable, subtasks in tileable_to_subtasks.items():
            tileable_dict[tileable.key] = [x.subtask_id for x in subtasks]
        save_path = os.path.join(task.session_id, task.task_id, "tileable.yaml")
        await self.save_yaml(tileable_dict, save_path)

    @collect_verify
    async def collect_runtime_subtask_info(self,
                                           subtask: Subtask,
                                           band: tuple,
                                           slot_id: int,
                                           stored_keys: list[str],
                                           store_sizes: Dict[str, int],
                                           memory_sizes: Dict[str, int],
                                           cost_times: Dict[str, Dict],
                                           ):
        subtask_dict = dict()
        subtask_dict["band"] = band
        subtask_dict["slot_id"] = slot_id
        subtask_dict.update(cost_times)
        result_chunks_dict = dict()
        stored_keys = list(set(stored_keys))

        for key in stored_keys:
            chunk_dict = dict()
            chunk_dict["memory_size"] = memory_sizes[key]
            chunk_dict["store_size"] = store_sizes[key]
            if isinstance(key, tuple):
                key = key[0]
            result_chunks_dict[key] = chunk_dict

        subtask_dict["result_chunks"] = result_chunks_dict
        save_path = os.path.join(subtask.session_id, subtask.task_id, f"subtask_runtime.yaml")
        await self.save_yaml({subtask.subtask_id: subtask_dict}, save_path)

    @collect_verify
    async def collect_runtime_operand_info(self,
                                           subtask: Subtask,
                                           execute_time: float,
                                           chunk: ChunkType,
                                           processor_context):
        op = chunk.op
        if isinstance(op, (Fetch, FetchShuffle)):
            return
        op_info = dict()
        op_key = op.key
        op_info["op_name"] = type(op).__name__
        op_info["subtask_id"] = subtask.subtask_id
        op_info["execute_time"] = execute_time
        if chunk.key in processor_context:
            op_info["memory_use"] = (calc_data_size(processor_context[chunk.key]))
            op_info["result_count"] = 1
        else:
            cnt = 0
            total_size = 0
            for k, v in processor_context.items():
                if isinstance(k, tuple) and k[0] == chunk.key:
                    cnt += 1
                    total_size += calc_data_size(v)
            op_info["memory_use"] = total_size
            op_info["result_count"] = cnt
        save_path = os.path.join(subtask.session_id, subtask.task_id, f"operand_runtime.yaml")
        await self.save_yaml({op_key: op_info}, save_path)

    @collect_verify
    async def collect_fetch_time(self,
                                 subtask: Subtask,
                                 fetch_start: float,
                                 fetch_end: float):
        save_path = os.path.join(subtask.session_id, subtask.task_id, "fetch_time.yaml")
        await self.save_yaml({subtask.subtask_id: {"fetch_time": {"start_time": fetch_start,
                                                                  "end_time": fetch_end}}}, save_path)

    async def save_yaml(self, obj, save_path):
        if self._cluster_api is None:
            self._cluster_api = await self._get_cluster_api()
        await self._cluster_api.save_yaml(obj, save_path)

