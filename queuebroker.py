import typing as t
import uuid
import time
from queue import Queue
from base import AbstractBroker, AbstractChannel, BrokerMessage

class Channel(AbstractChannel):
    def __init__(self, master_in: t.Optional[Queue[BrokerMessage]]=None, master_out: t.Optional[Queue[BrokerMessage]]=None):
        self.master_in = Queue() if master_in is None else master_in # type: Queue[BrokerMessage]
        self.master_out = Queue() if master_out is None else master_out # type: Queue[BrokerMessage]
    
    def replica(self) -> 'Channel':
        return Channel(self.master_out, self.master_in)
    
    def say(self, data: BrokerMessage) -> None:
        self.master_out.put(data)
    
    def listen(self) -> BrokerMessage:
        return self.master_in.get()
    
    def receive_noblock(self) -> t.Optional[BrokerMessage]:
        try:
            return self.master_in.get(block=False)
        except:
            return None
    
    def query(self, data: BrokerMessage) -> BrokerMessage:
        self.say(data)
        return self.listen()

class Broker(AbstractBroker):
    def __init__(self, timeout: t.Union[int, float]=1) -> None:
        self.timeout = timeout 
        self.clients = {} # type: t.Dict[str, Channel]
    
    def connect(self, name: t.Optional[str]=None) -> Channel:
        if name is None:
            name = str(uuid.uuid4())
        channel = Channel()
        self.clients[name] = channel
        return channel.replica()
    
    def run(self) -> None:
        while True:
            received_any = False
            for sender in self.clients:
                msg = self.clients[sender].receive_noblock() # type: t.Optional[BrokerMessage]
                if msg is not None:
                    received_any = True
                    msg.sender = sender
                    if msg.address is None:
                        for receiver in self.clients:
                            if receiver != sender:
                                self.clients[receiver].say(msg)
                    else:
                        self.clients[msg.address].say(msg)
            if not received_any:
                time.sleep(self.timeout)