import typing as t
from threading import Thread, Event
from concurrent.futures import ThreadPoolExecutor

class BrokerMessage(object):
    def __init__(self, data: t.Any, address: t.Optional[str]=None, sender: t.Optional[str]=None):
        self.sender = sender # type: t.Optional[str]
        self.address = address # type: t.Optional[str]
        self.data = data # type: t.Any

class AbstractChannel(object):
    def __init__(self) -> None:
        raise NotImplementedError

    def say(self, data: BrokerMessage) -> None:
        raise NotImplementedError

    def listen(self) -> BrokerMessage:
        raise NotImplementedError

    def receive_noblock(self) -> t.Optional[BrokerMessage]:
        raise NotImplementedError

    def query(self, data: BrokerMessage) -> BrokerMessage:
        self.say(data)
        return self.listen()

class AbstractBroker(object):
    def __init__(self) -> None:
        raise NotImplementedError
    
    def connect(self, name: t.Optional[str]=None) -> AbstractChannel:
        raise NotImplementedError

class BrokerClient(Thread):
    def __init__(self, broker: AbstractBroker, name: t.Optional[str]=None):
        self.channel = broker.connect(name) # type: AbstractChannel
        self.executor = ThreadPoolExecutor() # type: ThreadPoolExecutor
        self.quit_event = Event()
        super().__init__()
    
    def initialize(self) -> None:
        pass
    
    def on_message(self, message: BrokerMessage) -> None:
        raise NotImplementedError
    
    def say(self, address: t.Optional[str], data: t.Any) -> None:
        self.channel.say(BrokerMessage(data, address))
    
    def broadcast(self, data: t.Any) -> None:
        self.channel.say(BrokerMessage(data))
    
    def stop(self) -> None:
        self.quit_event.set()
    
    def run(self) -> None:
        self.initialize()
        while not self.quit_event.is_set():
            msg = self.channel.listen()
            self.executor.submit(self.on_message, msg)