from base.common.stream import Stream

class CSVFileStream(Stream):
    def __init__(self, service, **kwargs):
        file_path = kwargs.get('file_path', 'static/assets')
        super().__init__(service, file_path)
        seperator = kwargs.get('seperator', ',')
        schema = kwargs.get('schema')
        self.is_streaming = kwargs.get('is_streaming', True)
        if self.is_streaming:
            self._stream = self._stream.readStream
        else:
            self._stream = self._stream.read

        self._stream = self._stream.format("csv").option("sep", seperator).option('header', 'true')

        if schema:
            self._stream = self._stream.schema(schema)
        elif not self.is_streaming:
            self._stream = self._stream.option("inferSchema", "true")
