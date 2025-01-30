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

class TriggerFlowRuntimeData:
    def __init__(self, triggerflow):
        self._triggerflow = triggerflow
        self._data = {}
        self._result = None
    
    def set(self, key, value):
        self._data.update({ key: value })
        self._triggerflow._dispatch.emit(f"Data:{ key }", value)
        return self
    
    def get(self, key=None, default_value=None):
        if key is not None:
            return self._data[key] if key in self._data else default_value
        else:
            return self._data
    
    def key(self, key):
        return f"Data:{ key }"