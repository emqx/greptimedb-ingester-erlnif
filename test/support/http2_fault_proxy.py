"""Frame-preserving HTTP/2 proxy. No client or database responses are mocked."""
import socket
import threading
import time


def exact(sock, size):
    chunks = bytearray()
    while len(chunks) < size:
        part = sock.recv(size - len(chunks))
        if not part:
            raise EOFError
        chunks.extend(part)
    return bytes(chunks)


class FaultProxy:
    def __init__(self, backend):
        self.backend = backend
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen()
        self.listener.settimeout(0.2)
        self.port = self.listener.getsockname()[1]
        self.lock = threading.RLock()
        self.changed = threading.Condition(self.lock)
        self.paused = False
        self.stopped = False
        self.connections = []
        self.headers = set()
        self.held = []
        self.events = []
        self.errors = []
        self.backend_down = False
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()

    def accept(self):
        while not self.stopped:
            client = None
            try:
                client, _ = self.listener.accept()
                server = socket.create_connection(self.backend, timeout=5)
                server.settimeout(None)
                with self.lock:
                    key = len(self.connections)
                    self.connections.append((client, server))
                threading.Thread(target=self.pipe, args=(key, client, server, False), daemon=True).start()
                threading.Thread(target=self.pipe, args=(key, server, client, True), daemon=True).start()
            except socket.timeout:
                continue
            except OSError as exc:
                if client is not None:
                    client.close()
                if not self.stopped:
                    if self.backend_down:
                        self.events.append({'event': 'backend_unavailable', 'error': str(exc)})
                    else:
                        self.errors.append(str(exc))

    def pipe(self, key, source, destination, response):
        try:
            if not response:
                preface = exact(source, 24)
                if preface != b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n':
                    raise ValueError('not HTTP/2 prior knowledge')
                destination.sendall(preface)
            while not self.stopped:
                header = exact(source, 9)
                payload = exact(source, int.from_bytes(header[:3], 'big'))
                kind, flags = header[3:5]
                stream = int.from_bytes(header[5:], 'big') & 0x7fffffff
                packet = header + payload
                with self.changed:
                    if response and self.paused and stream:
                        if kind == 1 and not flags & 1:
                            destination.sendall(packet)
                            self.headers.add((key, stream))
                            self.events.append({'event': 'headers_forwarded', 'connection': key,
                                                'stream': stream, 'time': time.monotonic()})
                            self.changed.notify_all()
                        elif kind in (0, 1, 9):
                            self.held.append((destination, packet))
                            self.headers.add((key, stream))
                            self.events.append({"event": "body_held", "connection": key, "stream": stream, "time": time.monotonic()})
                            self.changed.notify_all()
                        else:
                            destination.sendall(packet)
                    else:
                        destination.sendall(packet)
        except (EOFError, OSError):
            pass
        except Exception as exc:
            self.errors.append(repr(exc))
        finally:
            for sock in (source, destination):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                    sock.close()
                except OSError:
                    pass

    def arm(self):
        with self.lock:
            self.paused = True
            self.headers.clear()

    def wait_headers(self, count, timeout=10):
        deadline = time.monotonic() + timeout
        with self.changed:
            while len(self.headers) < count:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f'only {len(self.headers)} of {count} stalled response headers')
                self.changed.wait(remaining)

    def release(self):
        with self.lock:
            self.paused = False
            for destination, packet in self.held:
                try:
                    destination.sendall(packet)
                except OSError:
                    pass
            self.held.clear()

    def disconnect(self):
        with self.lock:
            for pair in self.connections:
                for sock in pair:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                        sock.close()
                    except OSError:
                        pass
            self.paused = False
            self.held.clear()

    def close(self):
        self.stopped = True
        self.disconnect()
        self.listener.close()
        self.thread.join(timeout=1)
