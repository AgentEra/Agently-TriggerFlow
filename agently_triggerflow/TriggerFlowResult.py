import threading
from agently_stage import Tunnel

class TriggerFlowResult:
    def __init__(self):
        self._result = None
        self._result_ready = threading.Event()
        self._is_stop = False
    
    def put(self, item):
        if self._result is None:
            self._result = Tunnel()
        if not isinstance(self._result, Tunnel):
            raise ValueError("[Agently TriggerFlow] Can not put value to flow result after it was set value. Please check if you've already set value to flow result with `flow.result.set()`.")
        self._result.put(item)
        self._result_ready.set()
        return self
    
    def put_stop(self):
        if self._is_stop:
            raise ValueError("[Agently TriggerFlow] Can not repeat put stop to flow generator result.")
        self._is_stop = True
        return self.put(StopIteration)

    def set(self, result):
        if self._result is not None:
            raise ValueError("[Agently TriggerFlow] Can not repeat set flow result.")
        self._result = result
        self._result_ready.set()
    
    def get(self):
        self._result_ready.wait()
        return self._result