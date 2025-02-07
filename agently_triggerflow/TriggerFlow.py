# Copyright 2025 Maplemx(Mo Xin), AgentEra Ltd. Agently Team(https://Agently.tech)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Contact us: Developer@Agently.tech

from typing import Union, Callable
from agently_stage import Stage, Tunnel
from .TriggerFlowDispatch import TriggerFlowDispatch
from .TriggerFlowChunk import TriggerFlowTaskChunk, TriggerFlowWaitChunk, TriggerFlowIfConditionChunk, TriggerFlowElseConditionChunk
from .TriggerFlowRuntimeData import TriggerFlowRuntimeData
from .TriggerFlowResult import TriggerFlowResult

class TriggerFlow:
    CONTINUE_STOP = StopIteration
    _name_counter = 0

    def __init__(
            self,
            name=None,
            max_workers=None,
            exception_handler=None,
            is_debug=False,
        ):
        """
        Agently TriggerFlow create an trigger flow instance to manage event-driven tasks.

        Usage:

        ```
        from agently_triggerflow import TriggerFlow

        flow = TriggerFlow()

        @flow.chunk
        def first_step(trigger_data):
            # Get data from start input
            print("First Step Trigger Data:", trigger_data)
            return "First Step Done"

        @flow.chunk
        def second_step(trigger_data):
            # Get data from last chunk's return
            print("Second Step Trigger Data:", trigger_data)
            # Set data to runtime data
            flow.data.set("end", True)

        @flow.chunk
        def say_goodbye(_):
            print("Goodbye")
            # Set value as trigger flow's result
            flow.result.set("All Done")

        # Chunk tasks triggered by chunk function's execution
        flow.wait(first_step).then(second_step)
        # Chunk tasks triggered by runtime data update
        flow.on_data("end").then(say_goodbye).then(flow.END)

        # Start flow by appoint start chunk and assign start input trigger data
        result = flow.start(first_step, "Start")

        # Output flow's result and runtime data
        print(
            # Get result from trigger flow
            result,
            # Get data from runtime data
            flow.data.get()
        )
        ```
        """
        if name:
            self.__name__ = f"TriggerFlow-{ name }"
        else:
            TriggerFlow._name_counter += 1
            self.__name__ = f"TriggerFlow-{ TriggerFlow._name_counter }"
        self._stage = Stage(
            max_workers=max_workers,
            exception_handler=exception_handler,
        )
        self._dispatch = TriggerFlowDispatch(self)
        self.data = TriggerFlowRuntimeData(self)
        self._chunk_schemas = {}
        self._start_chunk = None
        self._id_counter = {}
        self.wait_all = self.wait_group
        self.result = TriggerFlowResult(name=f"Result:{ self.__name__ }")
        self.END = TriggerFlowTaskChunk(
            self,
            self._handle_end_chunk,
            name="END",
        )
        self._is_debug = is_debug
    
    def _handle_end_chunk(self, trigger_data):
        if self.result._result is None:
            self.result.set(trigger_data)
        elif isinstance(self.result._result, Tunnel) and self.result._is_stop == False:
            self.result.put_stop()

    def chunk(
            self,
            task: Union[Callable[[any], any], str, TriggerFlowTaskChunk, TriggerFlowIfConditionChunk, TriggerFlowElseConditionChunk],
            *,
            name: str=None,
            from_condition_chunk: TriggerFlowIfConditionChunk=None
        )->Union[TriggerFlowTaskChunk, TriggerFlowWaitChunk, TriggerFlowIfConditionChunk, TriggerFlowElseConditionChunk]:
        """
        Agently TriggerFlow Chunk

        Args:
        - `task` (`function` | `str` | `TriggerFlowChunk`): Create new chunk if task is a function or get an existed chunk if task is a string of chunk's name or a chunk.
        - `name` (`str`): [optional] Chunk name if a new chunk is created.
        - `from_condition_chunk` (`TriggerFlowIfConditionChunk`): A mark to help Agently TriggerFlow to locate from which chunk if condition start in chain syntax.

        Return:
        - When create a new chunk: TriggerFlowTaskChunk
        - When get an existed chunk: TriggerFlowTaskChunk | TriggerFlowWaitChunk | TriggerFlowIfConditionChunk | TriggerFlowElseConditionChunk

        Usage:
        ```python
        from agently_triggerflow import TriggerFlow()
        flow = TriggerFlow()

        # Create a chunk
        def chunk_func(trigger_data):
            pass
        demo_chunk_1 = flow.chunk(chunk_func, name="demo_chunk_1")
        
        # Create a chunk with function decorator
        @flow.chunk
        def demo_chunk_2(trigger_data):
            pass
        
        # Get an existed chunk
        another_demo_chunk_2 = flow.chunk("demo_chunk_2")
        ```
        """
        if isinstance(task, (TriggerFlowTaskChunk, TriggerFlowIfConditionChunk, TriggerFlowElseConditionChunk)):
            if from_condition_chunk is not None:
                task._from_condition_chunk = from_condition_chunk
            if self._start_chunk is None:
                self._start_chunk = task
            return task
        else:
            chunk = TriggerFlowTaskChunk(self, task, name=name, from_condition_chunk=from_condition_chunk)
            if self._start_chunk is None:
                self._start_chunk = chunk
            return chunk
    
    def mark_start_chunk(self, chunk: Union[TriggerFlowTaskChunk, TriggerFlowIfConditionChunk]):
        """
        Mark a task chunk or if-condition chunk as chunk to start

        Args:
        - `chunk` (`TriggerFlowTaskChunk` | `TriggerFlowIfConditionChunk`): Then chunk to start when `flow.start()`.
        """
        self._start_chunk = self.chunk(chunk)

    def wait_any(self, *args:Union[str, TriggerFlowTaskChunk])->TriggerFlowWaitChunk:
        """
        Wait if any event in args is emitted, then start to do next.

        Args:
        - `*args` (`str` | `TriggerFlowTaskChunk`):
            - wait for chunk done:
                - `(str)`: chunk name
                - `(TriggerFlowTaskChunk)`: chunk to wait
            - wait for data emit:
                - `(str)`: f"Data:{ data_key }"
            - wait for condition judgement:
                - `(str)`: f"If-{ condition_chunk_name }:True" or f"If-{ condition_chunk_name }:False"
        """
        if len(args) == 0:
            raise ValueError("[Agently TriggerFlow] .wait_any() requires at least 1 event.")
        return TriggerFlowWaitChunk(self, *args, type="any")

    def wait_group(self, *args)->TriggerFlowWaitChunk:
        """
        Wait when each time that all events in args are emitted, then start the flow after this wait chunk.
        At the same time, all events' data will be erased to wait all events are emitted again next time.
        
        Args:
        - `*args` (`str` | `TriggerFlowTaskChunk`):
            - wait for chunk done:
                - `(str)`: chunk name
                - `(TriggerFlowTaskChunk)`: chunk to wait
            - wait for data emit:
                - `(str)`: f"Data:{ data_key }"
            - wait for condition judgement:
                - `(str)`: f"If-{ condition_chunk_name }:True" or f"If-{ condition_chunk_name }:False"
        """
        if len(args) == 0:
            raise ValueError("[Agently TriggerFlow] .wait_group() requires at least 1 event.")
        return TriggerFlowWaitChunk(self, *args, type="group")
    
    def wait_update(self, *args)->TriggerFlowWaitChunk:
        """
        Wait until all events in args are emitted, then start the flow after this wait chunk.
        After all events are emitted for the first time, if any event in args is emitted, the flow after this wait chunk will be started again.
        
        Args:
        - `*args` (`str` | `TriggerFlowTaskChunk`):
            - wait for chunk done:
                - `(str)`: chunk name
                - `(TriggerFlowTaskChunk)`: chunk to wait
            - wait for data emit:
                - `(str)`: f"Data:{ data_key }"
            - wait for condition judgement:
                - `(str)`: f"If-{ condition_chunk_name }:True" or f"If-{ condition_chunk_name }:False"
        """
        if len(args) == 0:
            raise ValueError("[Agently TriggerFlow] .wait_update() requires at least 1 event.")
        return TriggerFlowWaitChunk(self, *args, type="update")
    
    def wait(self, *args):
        """
        Same as `.wait_any()` if only 1 argument in args
        Same as `.wait_group()` if 2 or more arguments in args
        
        Args:
        - `*args` (`str` | `TriggerFlowTaskChunk`):
            - wait for chunk done:
                - `(str)`: chunk name
                - `(TriggerFlowTaskChunk)`: chunk to wait
            - wait for data emit:
                - `(str)`: f"Data:{ data_key }"
            - wait for condition judgement:
                - `(str)`: f"If-{ condition_chunk_name }:True" or f"If-{ condition_chunk_name }:False"
        """
        if len(args) == 0:
            raise ValueError("[Agently TriggerFlow] .wait() requires at least 1 event.")
        if len(args) == 1:
            return self.wait_any(*args)
        else:
            return self.wait_group(*args)

    def on_data(self, *args)->TriggerFlowWaitChunk:
        """
        Wait of key / keys in args in runtime data to be set.
        
        Args:
        - `*args` (`str`): Key names in runtime data to be waited.
        """
        if len(args) == 0:
            raise ValueError("[Agently TriggerFlow] .wait_data() requires at least 1 key of runtime data.")
        keys = []
        for arg in args:
            keys.append(f"Data:{ arg }")
        if len(keys) == 1:
            return self.wait_any(*keys)
        else:
            return self.wait_group(*keys)

    def collect_data(self, key):
        if not isinstance(key, str):
            raise TypeError("[Agently TriggerFlow] .collect() require one target key name string of runtime data to collect.")
        return TriggerFlowWaitChunk(self, key, type="continue")
    
    def go(self, task, *args, **kwargs):
        return self._stage.go(task, *args, **kwargs)

    def start(self, task=None, trigger_data=None):
        try:
            if task is None and self._start_chunk is None:
                raise ValueError("[Agently TriggerFlow] You must mark a start chunk using `triggerflow.mark_start()` or point to a chunk or a task using parameter `task`.")
            start_chunk = self.chunk(task if task is not None else self._start_chunk)
            self._stage.get(start_chunk._execute, trigger_data)
            return self.result
        finally:
            self._stage.ensure_responses()