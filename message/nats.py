from message.message import Message

class NatsMessage(Message):
    def __init__(self, message):
        self._message = message

    def error(self):
        if self._message:
            return None
        else:
            return "NATS Message Error"

    def value(self):
        return self._message