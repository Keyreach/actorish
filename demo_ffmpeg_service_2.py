from bottle import Bottle, request
from threading import Thread
import json
import os
import subprocess
import uuid
import typing as t
from functools import partial

from base import BrokerableClass, BrokerMessage
from queuebroker import Broker, Channel

FFMPEG_EXEC = 'C:\\Users\\Ake\\ffmpeg\\bin\\ffmpeg.exe'
OUTPUT_DIR = 'C:\\Users\\Ake\\Downloads\\test'
WORKERS = 2
SERVER_PORT = 8088

class WorkerController(BrokerableClass):
    def __init__(self, broker: Broker) -> None:
        super().__init__(broker, 'ffmpeg-master')        
        self.workers = {} # type: t.Dict[str, t.Dict[str, t.Any]]
    
    def initialize(self) -> None:
        pass
    
    def set_status(self, sender: t.Optional[str], status: bool) -> None:
        if sender in self.workers:
            self.workers[sender]['status'] = status
        elif sender is not None:
            self.workers[sender] = dict(status=status)
    
    def job_done(self, sender: t.Optional[str], filename: str, output_filename: str) -> None:
        print('Converted file: {}'.format(filename))
    
    def list_workers(self, sender: t.Optional[str]) -> None:
        self.say(sender, self.workers)
    
    def job_start(self, sender: t.Optional[str], source_filename: str) -> None:
        for name in self.workers:
            if self.workers[name]['status'] == 'ready':
                self.say_to(name).start_conversion(source_filename)
                return

class FfmpegWorker(BrokerableClass):
    def __init__(self, broker: Broker, output_dir: str):
        super().__init__(broker)        
        self.proc = None # type: t.Optional[subprocess.Popen[bytes]]
        self.output_dir = output_dir # type: str
        # self.say('ffmpeg-master', ('set-status', 'ready'))
        self.say_to('ffmpeg-master').set_status('ready')
    
    def start_conversion(self, sender: t.Optional[str], filename: str) -> None:
        try:
            self.say_to('ffmpeg-master').set_status('busy')
            output_filename = os.path.join(self.output_dir, '{}.mp3'.format(uuid.uuid4()))
            self.proc = subprocess.Popen([
                FFMPEG_EXEC,
                '-v', 'quiet',
                '-i', filename,
                '-b:a', '128k',
                output_filename
            ])
            self.proc.wait()
            self.proc = None
            self.say_to('ffmpeg-master').job_done(filename, output_filename)
        except:
            print('Worker error')
            import traceback
            traceback.print_exc()
        finally:
            self.say_to('ffmpeg-master').set_status('ready')       
        
    def stop_conversion(self, sender: t.Optional[str]) -> None:
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
        ('list-workers', ),
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