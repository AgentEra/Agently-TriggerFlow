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

class TriggerFlowDispatch:
    def __init__(self, triggerflow):
        self._triggerflow = triggerflow
        self._signals = {
            "single": {},
            "group": {},
            "update": {},
            "continue": {},
        }
        self._NOT_EMITTED = object()
    
    def check(self):
        result = {}
        for signal_type, signals in self._signals.items():
            result.update({signal_type: {}})
            for events, plans in signals.items():
                result[signal_type].update({ events: [] })
                if isinstance(plans, dict) and "then" in plans:
                    nexts = plans["then"]
                else:
                    nexts = plans
                for next in nexts:
                    result[signal_type][events].append(next._name)
        return result
    
    def wait_any(self, events, then):
        if isinstance(events, str):
            events = [events]
        for event in events:
            if event not in self._signals["single"]:
                self._signals["single"].update({ event: [] })
            self._signals["single"][event].append(then)
    
    def wait_group(self, events, then):
        if isinstance(events, str):
            events = [events]
        events = tuple(events)
        if events not in self._signals["group"]:
            empty_event_values = {}
            for event in events:
                empty_event_values.update({ event: [] })
            self._signals["group"].update({
                events: {
                    "event_values": empty_event_values,
                    "then": [],
                }
            })
        self._signals["group"][events]["then"].append(then)
    
    def wait_update(self, events, then):
        if isinstance(events, str):
            events = [events]
        events = tuple(events)
        if events not in self._signals["update"]:
            empty_event_values = {}
            for event in events:
                empty_event_values.update({ event: self._NOT_EMITTED })
            self._signals["update"].update({
                events: {
                    "event_values": empty_event_values,
                    "ready": False,
                    "then": [],
                }
            })
        self._signals["update"][events]["then"].append(then)
    
    def wait_continue(self, key, then):
        event = f"Data:{ key }"
        if event not in self._signals["continue"]:
            self._signals["continue"].update({
                event: {
                    "results": [],
                    "then": [],
                }
            })
        self._signals["continue"][event]["then"].append(then)
    
    def emit(self, event, data=None):
        #print("EVENT:", event, "DATA:", data)
        # Single
        if event in self._signals["single"]:
            for then in self._signals["single"][event]:
                #print("EXECUTE:", then._name)
                self._triggerflow.chunk(then)._execute({ event: data })
        # Group
        for events, events_info in self._signals["group"].items():
            if event in events:
                events_info["event_values"][event].append(data)
            can_execute = True
            for event, value in events_info["event_values"].items():
                if len(value) == 0:
                    can_execute = False
                    break
            if can_execute:
                event_values = {}
                for event, value in events_info["event_values"].items():
                    event_values.update({ event: value.pop(0) })
                for then in events_info["then"]:
                    #print("EXECUTE:", then._name)
                    self._triggerflow.chunk(then)._execute(event_values)
        # Update
        for events, events_info in self._signals["update"].items():
            if event in events:
                events_info["event_values"].update({ event: data })
            if not events_info["ready"]:
                is_ready = True
                for event, value in events_info["event_values"].items():
                    if value is self._NOT_EMITTED:
                        is_ready = False
                        break
                events_info["ready"] = is_ready
            if events_info["ready"]:
                for then in events_info["then"]:
                    #print("EXECUTE:", then._name)
                    self._triggerflow.chunk(then)._execute(events_info["event_values"])
        # Continue
        if event in self._signals["continue"]:
            if data is StopIteration:
                for then in self._signals["continue"][event]["then"]:
                    #print("EXECUTE:", then._name)
                    self._triggerflow.chunk(then)._execute(self._signals["continue"][event]["results"])
            else:
                self._signals["continue"][event]["results"].append(data)