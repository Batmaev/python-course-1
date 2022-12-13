import asyncio
import logging
import asyncio
from socketserver import TCPServer
from threading import Thread
from time import sleep
import numpy as np

from stem.envelope import Envelope
from stem.meta import get_meta_attr


class Distributor:

    server: asyncio.Server = NotImplemented

    def __init__(self, servers: list[TCPServer], recv_size = 1024):
        self.servers = servers
        self.free_servers = servers.copy()
        self.recv_size = recv_size
        self.free_servers_list_lock = asyncio.Lock()

    async def __call__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        logging.debug('Distributor.__call__ invoked')
        request = await Envelope.async_read(reader)

        match get_meta_attr(request.meta, 'command'):
            case 'run' | 'structure':

                async with self.free_servers_list_lock:
                    server = self.select_server()

                if server is None:
                    response = Envelope({'status': 'failed', 'error': 'no free servers'}).to_bytes()
                else:
                    server.socket.sendall(request.to_bytes())
                    response = server.socket.recv(self.recv_size)

                    async with self.free_servers_list_lock:
                        self.free_servers.append(server)


            case 'powerfullity':
                response = Envelope({
                    'status': 'fulfilled',
                    'powerfullity': sum(server.RequestHandlerClass.powerfullity for server in self.servers)
                }).to_bytes()

            case 'stop':
                writer.close()
                self.server.close()
                logging.info('server was closed on request')
                return

            case _:
                response = Envelope({
                    'status': 'failed',
                    'error' : 'command = ??'
                }).to_bytes()

        logging.debug(f'distributor sends: {response}')
        writer.write(response)
        await writer.drain()
        writer.close()


    def select_server(self) -> TCPServer | None:
        if len(self.free_servers) >= 1:
            weights = np.array([server.RequestHandlerClass.powerfullity for server in self.free_servers])
            weights /= sum(weights)
            selected_server = np.random.choice(np.array(self.free_servers), 1, p = weights)[0]
            self.free_servers.remove(selected_server)
            return selected_server



async def start_distributor(host: str, port: int, servers: list[TCPServer], save_server_at: list):
    distributor = Distributor(servers)
    server = await asyncio.start_server(distributor, host, port)
    distributor.server = server
    save_server_at.append(server)
    try:
        await server.serve_forever()
    except asyncio.CancelledError:
        logging.info('дистрибьютор своё отслужил')
        # Обычно надо reraise CancelledError,
        # но, надеюсь, здесь это не обязательно



def start_distributor_in_subprocess(host: str, port: int, servers: list[TCPServer]) -> tuple[Thread, asyncio.Server]:
    save_server_at = []
    thread = Thread(target = lambda : asyncio.run(start_distributor(host, port, servers, save_server_at)))
    thread.start()
    sleep(0.1) # wait for server to start and to be written in the list
    return thread, save_server_at[0]


# start_server() и serve_forever() должны быть запущены внутри одного event loop,
# то есть внутри одного asyncio.run().
# 
# Чтобы выключить сервер, нужно будет выполнить server.close(),
# поэтому нужно как-то вытащить и сохранить переменную server,
# при том, что функция start_distributor сама по себе не завершается
#
# Я делаю это двумя способами:
#   1. Мутируя distributor, чтобы потом можно было остановить сервер, отправив ему {'command': 'stop'}
#   2. Мутируя вспомогательный массив и в конечном счёте возвращая server из функции start_distributor_in_subprocess
