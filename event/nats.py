from logging import Logger
from typing import List
from event.eventbroker import EventBroker
from config.nats import NatsSettings

class NatsClient(EventBroker):
    def __init__(self, config: NatsSettings, channels: List[str], logger: Logger):
        super().__init__(config, channels, logger)
        # TODO Implement NATS client
        pass

    def publish(self, channel: str, data):
        # TODO Implement NATS publish
        pass

    def poll(self, consumer, timeout):
        # TODO Implement NATS poll
        pass

    def subscribe(self, channel: str, group_id: str = None):
        # TODO Implement NATS subscribe
        pass

    def get_sibling_channels(self):
        return self.topics