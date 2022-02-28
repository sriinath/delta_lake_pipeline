class DeltaTable:
    def __init__(self, input_source, **kwargs):
        self.is_streaming = kwargs.get('is_streaming', True)
        is_delta = kwargs.get('is_delta', False)
        sink_format = "delta" if is_delta else "console"

        if self.is_streaming:
            self.__delta_stream = input_source.writeStream
        else:
            self.__delta_stream = input_source.write

        checkpoint_location = kwargs.get("checkpoint_location", "target/check_point")
        self.file_path = kwargs.get('file_path', 'target/delta')

        self.__delta_stream = self.__delta_stream.format(sink_format) \
        .option("checkpointLocation", checkpoint_location) \
        .option("header", 'true')


    def get_sink(self):
        return self.__delta_stream


    def start(self):
        if self.is_streaming:
            self.__delta_stream = self.__delta_stream.option("path", self.file_path).start()
        else:
            self.__delta_stream.save(self.file_path)
        
        return self.__delta_stream
