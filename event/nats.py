from logging import Logger
from typing import Dict, List, Optional, Union
from event.eventbroker import EventBroker
from config.nats import NatsSettings
from message.message import Message
from message.nats import NatsMessage
import nats_sync
from nats_sync.nats_ctx import NATSSub, NATSContext
import json

class NatsClient(EventBroker):
    def __init__(self, config: NatsSettings, channels: List[str], logger: Logger):
        super().__init__(config, channels, logger)
        self.client: NATSContext = nats_sync.connect(f"nats://{config.host}:{config.port}")
        # There is no need to initialize subjects (topics / channels) in NATS
        self.subjects: List[str] = channels
        self.logger: Logger = logger
        self.subscribers: Dict[str, NATSSub] = dict()

    def publish(self, channel: str, data):
        if channel not in self.subjects:
            self.logger.warning(f"NATS subject {channel} is an unknown subject")
        serialized_data: str = json.dumps(data, default=lambda x: "<not serializable>")
        self.logger.info(f"Publishing message to NATS subject {channel}: {serialized_data}")
        self.client.publish(channel, bytes(serialized_data, 'utf-8'))

    def poll(self, consumer: NATSSub, timeout) -> Optional[Message]:
        try:
            # Returns a NATS Msg Object, which is wrapped
            message = consumer.recv(timeout)
        except TimeoutError:
            message = None
        except Exception as exception:
            self.logger.warning(f"Unhandled exception when polling for NATS subject {consumer._sub.subject}: {exception}")
            message = None
        return NatsMessage(message)

    def subscribe(self, channel: str, group_id: str = None):
        if channel not in self.subscribers.keys():
            subscriber: NATSSub = self.client.subscribe(channel)
            self.subscribers.update({channel: subscriber})
            self.logger.info(f"Subscribed to NATS subject {channel}")
        else:
            self.logger.warning(f"Tried to subscribe to NATS subject with active subscription: {channel}")
        return self.subscribers[channel], channel

    def get_sibling_channels(self):
        return self.subjects
    
    def close(self):
        # Unsubscribe from all subjects
        for (subject, subscriber) in self.subscribers:
            self.close_consumer(subject, subscriber)
        self.logger.info("All NATS subscribers closed")
        # Delete all subjects
        del self.subjects
        self.logger.info("Closing all NATS subjects")
        # Shut down client
        self.client._nc.flush()
        self.client._nc.close()
        self.logger.info("Closed NATS client")            

    def close_consumer(self, key: str):
        if key in self.subscribers.keys():
            self.subscribers[key].unsubscribe()
            self.logger.info(f"Subscriber for NATS subject {key} closed.")
            del self.subscribers[key]
        else:
            self.logger.warning(f"Unable to close subscriber for NATS subject {key}: Subscriber not found")

    # This method is not used publicly
    def new_sibling_channel(self, channel: str):
        pass