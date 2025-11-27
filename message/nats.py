from message.message import Message

class NatsMessage(Message):
    def __init__(self, message):
        self._message = message

    def error(self):
        pass  # TODO: Implement error handling if applicable

    def value(self):
        pass # TODO: Adjust decoding as necessary