import typing as t
from threading import Thread, Event
from functools import partial
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

class Message(object):
    def __init__(self, channel: AbstractChannel, to: t.Optional[str]=None):
        self.address = to
        self.channel = channel
    
    def _get(self, command: str, *args: t.List[t.Any]) -> None:
        method_name = command.replace('_', '-')
        self.channel.say(BrokerMessage((method_name, *args), self.address))
    
    def __getattribute__(self, command: str) -> t.Any:
        if command in ('address', '_get', 'channel'):
            return object.__getattribute__(self, command)
        return partial(self._get, command)

class BrokerableClass(BrokerClient):
    def say_to(self, address: t.Optional[str]=None) -> Message:
        return Message(self.channel, address)
        
    def run(self) -> None:
        while not self.quit_event.is_set():
            msg = self.channel.listen()
            command, *arguments = msg.data
            method_name = command.replace('-', '_')
            if hasattr(self, method_name):
                self.executor.submit(getattr(self, method_name), msg.sender, *arguments)
            else:
                print('Method {} not found'.format(method_name))
        