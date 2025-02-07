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

from agently_stage import Stage, StageResponse, StageHybridGenerator

class BaseTriggerFlowChunk:
    _empty_task = lambda trigger_data: trigger_data

    def __init__(self, triggerflow, task=None, *, name=None, from_condition_chunk=None):
        self._triggerflow = triggerflow
        self._stage = self._triggerflow._stage
        self._dispatch = self._triggerflow._dispatch
        if task is None and name is None:
            raise ValueError("[Agently TriggerFlow] You must provide a task or a name to the chunk.")
        if isinstance(task, str):
            name = task
            task = None
        if task is None:
            if name in self._triggerflow._chunk_schemas:
                task = self._triggerflow._chunk_schemas[name]["task"]
            else:
                raise ValueError(f"[Agently TriggerFlow] Can not find task named '{ name }'")
        else:
            if name is None:
                name = task.__name__
            name = self._generate_unique_name(name)
            if name not in self._triggerflow._chunk_schemas:
                self._triggerflow._chunk_schemas.update({ name: {} })
            self._triggerflow._chunk_schemas[name].update({ "task": task })
        self._schema = self._triggerflow._chunk_schemas[name]
        self._task = task
        self._name = name
        self._from_condition_chunk = from_condition_chunk
    
    def _generate_unique_name(self, name:str):
        if (
            name not in self._triggerflow._chunk_schemas
            and name not in ("if", "else", "end_condition", "<lambda>")
        ):
            return name
        else:
            if name not in self._triggerflow._id_counter:
                self._triggerflow._id_counter.update({ name: 0 })
            self._triggerflow._id_counter[name] += 1
            return f"{ name }-{ self._triggerflow._id_counter[name] }"
    
    def _execute(self, events):
        if isinstance(events, dict) and len(events) == 1:
            events = events[list(events.keys())[0]]
        self._dispatch.emit(f"Start:{ self._name }", events)
        task_stage_func = self._stage.func(self._task)
        task_stage_func(events)
            
        if type(task_stage_func._response) is StageResponse:
            def _response_handler():
                result = task_stage_func.wait()
                self._dispatch.emit(self._name, result)
            self._stage.go(_response_handler)
        elif type(task_stage_func._response) is StageHybridGenerator:
            def _response_handler():
                for result in task_stage_func.wait():
                    self._dispatch.emit(self._name, result)
            self._stage.go(_response_handler)
    
    def then(self, task=None, *, name=None):
        next_chunk = self._triggerflow.chunk(task, name=name, from_condition_chunk=self._from_condition_chunk)
        self._dispatch.wait_any(f"{ self._name }", next_chunk)
        return next_chunk
    
    def if_condition(self, condition, *, name=None):
        next_chunk = TriggerFlowIfConditionChunk(self._triggerflow, condition, name=name, from_condition_chunk=self._from_condition_chunk)
        self._dispatch.wait_any(f"{ self._name }", next_chunk)
        return next_chunk

    def else_condition(self, *, name=None):
        if self._from_condition_chunk is None:
            raise ReferenceError("[Agently TriggerFlow] .else_condition() can only be used after a .if_condition().")
        next_chunk = TriggerFlowElseConditionChunk(self._triggerflow, name=name, from_condition_chunk=self._from_condition_chunk)
        self._from_condition_chunk._end_condition_chunks.append(self._name)
        return next_chunk
    
    def end_condition(self):
        if self._from_condition_chunk is None:
            raise ReferenceError("[Agently TriggerFlow] .end_condition() can only be used after a .if_condition().")
        next_chunk = TriggerFlowTaskChunk(self._triggerflow, BaseTriggerFlowChunk._empty_task, name="end_condition", from_condition_chunk=self._from_condition_chunk._from_condition_chunk)
        self._from_condition_chunk._end_condition_chunks.append(self._name)
        self._dispatch.wait_any(self._from_condition_chunk._end_condition_chunks, next_chunk)
        return next_chunk

class TriggerFlowTaskChunk(BaseTriggerFlowChunk):
    def __init__(self, triggerflow, task=None, *, name=None, from_condition_chunk=None):
        super().__init__(triggerflow, task, name=name, from_condition_chunk=from_condition_chunk)
        self.to = self.then

class TriggerFlowWaitChunk(BaseTriggerFlowChunk):
    def __init__(self, triggerflow, *args, name=None, type="any"):
        if name is None:
            name = f"wait_{ type }"
        super().__init__(triggerflow, BaseTriggerFlowChunk._empty_task, name=name)
        self._wait_events = []
        for event in args:
            if isinstance(event, str):
                self._wait_events.append(event)
            elif isinstance(event, TriggerFlowTaskChunk):
                self._wait_events.append(event._name)
        self._type = type
        self.to = self.then
    
    def _add_dispatch_wait(self, next_chunk):
        if self._type == "any":
            self._dispatch.wait_any(self._wait_events, next_chunk)
        elif self._type == "group":
            self._dispatch.wait_group(self._wait_events, next_chunk)
        elif self._type == "update":
            self._dispatch.wait_update(self._wait_events, next_chunk)
        elif self._type == "continue":
            self._dispatch.wait_continue(self._wait_events[0], next_chunk)
    
    def then(self, task=None, *, name=None):
        next_chunk = self._triggerflow.chunk(task, name=name)
        self._add_dispatch_wait(next_chunk)
        return next_chunk

    def if_condition(self, condition, *, name=None):
        next_chunk = TriggerFlowIfConditionChunk(self._triggerflow, condition, name=name, from_condition_chunk=self._from_condition_chunk)
        self._add_dispatch_wait(next_chunk)
        return next_chunk


class TriggerFlowIfConditionChunk(BaseTriggerFlowChunk):
    def __init__(self, triggerflow, condition, *, name=None, from_condition_chunk=None):
        name = "if" if name is None else f"if-{ name }"
        super().__init__(triggerflow, BaseTriggerFlowChunk._empty_task, name=name, from_condition_chunk=from_condition_chunk)
        self._condition = condition
        self._end_condition_chunks = []
        self.to = self.then
    
    def _execute(self, events):
        if isinstance(events, dict) and len(events) == 1:
            events = events[list(events.keys())[0]]
        judge_stage_func = self._stage.func(self._condition)
        def _handle_judgement():
            judgement = judge_stage_func.wait()
            if judgement:
                self._dispatch.emit(f"{ self._name }:True", events)
            else:
                self._dispatch.emit(f"{ self._name }:False", events)
        self._stage.go(_handle_judgement)
        judge_stage_func(events)
    
    def then(self, task=None, *, name=None):
        next_chunk = self._triggerflow.chunk(task, name=name, from_condition_chunk=self)
        self._dispatch.wait_any(f"{ self._name }:True", next_chunk)
        return next_chunk

class TriggerFlowElseConditionChunk(BaseTriggerFlowChunk):
    def __init__(self, triggerflow, *, name=None, from_condition_chunk=None):
        name = "else" if name is None else f"else-{ name }"
        super().__init__(triggerflow, BaseTriggerFlowChunk._empty_task, name=name, from_condition_chunk=from_condition_chunk)
        self.to = self.then
    
    def then(self, task=None, *, name=None):
        next_chunk = self._triggerflow.chunk(task, name=name, from_condition_chunk=self._from_condition_chunk)
        self._dispatch.wait_any(f"{ self._from_condition_chunk._name }:False", next_chunk)
        return next_chunk