from base.common.stream import Stream

class DeltaStream(Stream):
    def __init__(self, service, **kwargs):
        file_path = kwargs.get('file_path', 'static/assets')
        is_streaming = kwargs.get('is_streaming', True)
        super().__init__(service, file_path)
        if is_streaming:
            self._stream = self._stream.readStream.format("delta")
        else:
            self._stream = self._stream.read.format("delta")
