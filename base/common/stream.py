class Stream:
    def __init__(self, service, file_path):
        self._stream = service
        self.file_path = file_path


    def get_source(self):
        return self._stream


    def load(self):
        return self._stream.load(self.file_path)
