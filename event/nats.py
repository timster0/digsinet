from logging import Logger
from typing import Dict, List
from event.eventbroker import EventBroker
from config.nats import NatsSettings
from message.message import Message
from message.nats import NatsMessage
import nats_sync
from nats_sync.nats_ctx import NATSSub
import json

class NatsClient(EventBroker):
    def __init__(self, config: NatsSettings, channels: List[str], logger: Logger):
        super().__init__(config, channels, logger)
        self.client = nats_sync.connect(f"nats://{config.host}:{config.port}")
        # There is no need to initialize subjects (topics / channels) in NATS
        self.subjects: List[str] = channels
        self.logger: Logger = logger
        self.subscribers: Dict[str, NATSSub] = dict()

    def publish(self, channel: str, data):
        if channel not in self.subjects:
            self.logger.warning(f"NATS Topic {channel} is an unknown channel")
        serialized_data: str = json.dumps(data, default=lambda x: "<not serializable>")
        self.logger.info(f"Publishing message to NATS subject {channel}: {serialized_data}")
        self.client.publish(channel, bytes(serialized_data))

    def poll(self, consumer, timeout) -> Message:
        self.client
        # TODO Implement NATS poll
        pass

    def subscribe(self, channel: str, group_id: str = None):
        if channel not in self.subscribers.keys():
            self.client.subscribe(channel)
        else:
            self.logger.warning(f"Tried to subscribe to channel with active subscription: {channel}")

    def get_sibling_channels(self):
        return self.subjects