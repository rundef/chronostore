from pattern_kit import DelegateMixin

from .backend.base import Backend

class TimeSeriesEngine(DelegateMixin):
    def __init__(self, backend: Backend):
        self.backend = backend
        self._delegate_methods(backend)
