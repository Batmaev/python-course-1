import json
import logging
from socketserver import StreamRequestHandler, TCPServer
from threading import Thread
from multiprocessing import Process
from typing import Optional, Tuple, Type

from stem.envelope import Envelope
from stem.task_master import TaskMaster, TaskStatus
from stem.task_tree import TaskTree
from stem.workspace import IWorkspace
from stem.meta import get_meta_attr


class UnitHandler(StreamRequestHandler):
    workspace: IWorkspace | Type[IWorkspace]
    task_tree: TaskTree | None
    task_master: TaskMaster
    powerfullity: int | None

    def handle(self) -> None:
        logging.debug('handle entered')
        self.envelope = Envelope.read(self.rfile)

        logging.debug(f"server receives command: {get_meta_attr(self.envelope.meta, 'command')}")

        match get_meta_attr(self.envelope.meta, 'command'):
            case 'run':
                task_path = get_meta_attr(self.envelope.meta, 'task_path')
                task = self.workspace.find_task(task_path)
                if task is None:
                    resp = Envelope({
                        'status': 'failed',
                        'error' : 'Task not found'
                    })
                else:
                    res = self.task_master.execute(
                        get_meta_attr(self.envelope.meta, 'task_meta', {}),
                        task,
                        self.workspace
                    )
                    if res.status == TaskStatus.CONTAINS_DATA:
                        data = res.lazy_data()
                        resp = Envelope(
                            {'status': 'fulfilled'},
                            json.dumps(data).encode('utf8')
                        )
                    else:
                        resp = Envelope({
                            'status': 'failed',
                            'error' : res.status
                        })

            case 'structure':
                structure = self.workspace.structure()
                resp = Envelope(
                    {'status': 'fulfilled'},
                    json.dumps(structure).encode('utf8')
                )

            case 'powerfullity':
                resp = Envelope({
                    'status': 'fulfilled',
                    'powerfullity': self.powerfullity
                })

            # case 'stop':
            #     resp = Envelope({'status': 'recieved'})               # не работает
            #     self.wfile.write(resp.to_bytes())
            #     # We should call server.shutdown from a new thread,
            #     # otherwise it will result in deadlock
            #     thread = Thread(target = self.server.shutdown)
            #     thread.start()
            #     thread.join()
            #     return

            case _:
                resp = Envelope({
                    'status': 'failed',
                    'error' : 'input_envelope.meta.command = ???'
                })

        logging.info(f'server sends: {resp.meta}')

        self.wfile.write(resp.to_bytes()) # если вместо этого написать resp.write_to(self.wfile),
        self.wfile.flush()                # то будет несколько вызовов wfile.write,
                                          # но клиент отключается после первого


def start_unit(workspace: IWorkspace, host: str, port: int, powerfullity: int | None = None) -> TCPServer:
    UnitHandler.workspace = workspace
    UnitHandler.task_tree = None
    UnitHandler.task_master = TaskMaster()
    UnitHandler.powerfullity = powerfullity
    TCPServer.allow_reuse_address = True # иначе нужно будет ждать минуту, прежде чем перезапускать сервер
    return TCPServer((host, port), UnitHandler)


def start_unit_in_subprocess(workspace: IWorkspace, host: str, port: int, powerfullity: Optional[int] = None) -> Tuple[Thread, TCPServer]:
    server = start_unit(workspace, host, port, powerfullity)
    thread = Thread(target = server.serve_forever)
    thread.start()

    logging.debug('thread started')

    return thread, server

# в process не получилось, т.к. pickle
