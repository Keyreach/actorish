from typing import Optional, Dict
import redis
import uuid
import json
from base import BrokerMessage, AbstractChannel, AbstractBroker

class Channel(AbstractChannel):
    def __init__(self, redis_url: str, prefix: str, name: Optional[str]=None):
        if name is None:
            self.name = str(uuid.uuid4())
        else:
            self.name = name
        self.r = redis.from_url(redis_url)
        self.p = self.r.pubsub()
        self.prefix = prefix
        self.p.subscribe([
            '{}:{}'.format(prefix, self.name),
            '{}:all'.format(prefix)
        ])
    
    def serialize(self, msg: BrokerMessage) -> str:
        return json.dumps({
            'from': self.name,
            'to': msg.address,
            'body': msg.data
        })
    
    def deserialize(self, msg: str) -> BrokerMessage:
        data = json.loads(msg)
        return BrokerMessage(data['body'], data['to'], data['from'])
    
    def say(self, data: BrokerMessage) -> None:
        if data.address is None:
            recv_channel = '{}:all'.format(self.prefix)
        else:
            recv_channel = '{}:{}'.format(self.prefix, data.address)
        self.r.publish(recv_channel, self.serialize(data))

    def listen(self) -> BrokerMessage:
        # while True:
        #     msg = self.p.get_message()
        #     if msg is not None and msg['type'] == 'message':
        #         return self.deserialize(msg['data'])
        #     time.sleep(1)
        while True:
            msg = next(self.p.listen()) # type: Dict[str, str]
            if msg['type'] == 'message':
                break
        return self.deserialize(msg['data'])

    def receive_noblock(self) -> Optional[BrokerMessage]:
        msg = self.p.get_message()
        if msg is not None and msg['type'] == 'message':
            return self.deserialize(msg['data'])
        return None

    def query(self, data: BrokerMessage) -> BrokerMessage:
        self.say(data)
        return self.listen()


class Broker(AbstractBroker):
    def __init__(self, redis_url: str, prefix: str):
        self.redis_url = redis_url
        self.prefix = prefix
    
    def connect(self, name: Optional[str]=None) -> Channel:
        return Channel(self.redis_url, self.prefix, name)

