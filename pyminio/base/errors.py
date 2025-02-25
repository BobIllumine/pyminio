

class BackendNotSupportedError(Exception):
    def __init__(self, backend: str):
        self.message = f'Backend {backend} is not supported yet'
        super().__init__(self.message)

