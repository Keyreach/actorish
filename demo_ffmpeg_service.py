from bottle import Bottle, request
from threading import Thread
import json
import os
import subprocess
import uuid
import typing as t
from functools import partial

from base import BrokerClient, BrokerMessage
from queuebroker import Broker, Channel

FFMPEG_EXEC = 'C:\\Users\\Ake\\ffmpeg\\bin\\ffmpeg.exe'
OUTPUT_DIR = 'C:\\Users\\Ake\\Downloads\\test'
WORKERS = 2
SERVER_PORT = 8088

class WorkerController(BrokerClient):
    def __init__(self, broker: Broker):
        self.workers = {} # type: t.Dict[str, t.Dict[str, t.Any]]
        super().__init__(broker, 'ffmpeg-master')
    
    def initialize(self) -> None:
        pass
    
    def on_message(self, message: BrokerMessage) -> None:
        print('master got message', message.data)
        command = message.data[0]
        if command == 'set-status':
            status = message.data[1]
            if message.sender in self.workers:
                self.workers[message.sender]['status'] = status
            elif message.sender is not None:
                self.workers[message.sender] = dict(status=status)
        elif command == 'job-done':
            print('Converted file: {}'.format(message.data[1]))
        elif command == 'workers':
            self.say(message.sender, self.workers)
        elif command == 'job-start':
            source_filename = message.data[1]
            for name in self.workers:
                if self.workers[name]['status'] == 'ready':
                    self.say(name, {'command': 'start', 'input': source_filename})
                    return
        else:
            print('Unknown message')

class FfmpegWorker(BrokerClient):
    def __init__(self, broker: Broker, output_dir: str):
        self.proc = None # type: t.Optional[subprocess.Popen[bytes]]
        self.output_dir = output_dir # type: str
        super().__init__(broker)
    
    def initialize(self) -> None:
        self.say('ffmpeg-master', ('set-status', 'ready'))
    
    def on_message(self, message: BrokerMessage) -> None:
        if message.data['command'] == 'start':
            try:
                self.say('ffmpeg-master', ('set-status', 'busy'))
                output_filename = os.path.join(self.output_dir, '{}.mp3'.format(uuid.uuid4()))
                self.proc = subprocess.Popen([
                    FFMPEG_EXEC,
                    '-v', 'quiet',
                    '-i', message.data['input'],
                    '-b:a', '128k',
                    output_filename
                ])
                self.proc.wait()
                self.proc = None
                self.say('ffmpeg-master', ('job-done', message.data['input'], output_filename))
            except:
                print('Worker error')
                import traceback
                traceback.print_exc()
            finally:
                self.say('ffmpeg-master', ('set-status', 'ready'))
        elif message.data['command'] == 'stop':
            if self.proc is not None:
                self.proc.terminate()
                self.proc = None

app = Bottle()

broker = Broker(0.1)
iface = broker.connect()

@app.post('/ffmpeg/start')
def run_ffmpeg() -> str:
    req = request.json
    iface.say(BrokerMessage(
        ('job-start', req['source']),
        'ffmpeg-master'
    ))
    return json.dumps({
        'success': True
    })

@app.get('/ffmpeg/status')
def status_ffmpeg() -> str:
    workers = iface.query(BrokerMessage(
        ('workers', ),
        'ffmpeg-master'
    )).data
    return json.dumps({
        'success': True,
        'workers': workers
    })

if __name__ == '__main__':
    controller = WorkerController(broker)
    controller.start()
    for i in range(WORKERS):
        worker = FfmpegWorker(broker, OUTPUT_DIR)
        worker.start()
    Thread(target=broker.run).start()
    app.run(port=SERVER_PORT)