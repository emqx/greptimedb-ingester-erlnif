#!/usr/bin/env python3
"""Exercise the real Erlang API/NIF through a transparent HTTP/2 fault proxy."""
import argparse
import json
import os
from pathlib import Path
import select
import tempfile
import urllib.request
import socket
import subprocess
import time

from http2_fault_proxy import FaultProxy

ROOT = Path(__file__).resolve().parents[2]


class Control:
    def __init__(self, sock):
        self.sock, self.buffer = sock, b''

    def send(self, command):
        self.sock.sendall(command.encode() + b'\n')

    def read(self, timeout=10):
        end = time.monotonic() + timeout
        while b'\n' not in self.buffer:
            remaining = end - time.monotonic()
            if remaining <= 0 or not select.select([self.sock], [], [], remaining)[0]:
                raise TimeoutError('BEAM control response missing')
            part = self.sock.recv(65536)
            if not part:
                raise EOFError('BEAM control connection closed')
            self.buffer += part
        line, self.buffer = self.buffer.split(b'\n', 1)
        return json.loads(line)

    def snapshot(self):
        self.send('snapshot')
        return self.read()


class Database:
    def __init__(self, binary, port):
        self.binary, self.port = binary, port
        self.directory = tempfile.TemporaryDirectory(prefix='greptime-io-regression-')
        self.process = None
        self.log = open(Path(self.directory.name) / 'database.log', 'w')

    def start(self):
        self.process = subprocess.Popen([self.binary, 'standalone', 'start',
            '--rpc-bind-addr', f'127.0.0.1:{self.port}',
            '--http-addr', f'127.0.0.1:{self.port-1}',
            '--mysql-addr', f'127.0.0.1:{self.port+1}',
            '--postgres-addr', f'127.0.0.1:{self.port+2}',
            '--data-home', self.directory.name + '/data',
            '--log-dir', self.directory.name + '/logs', '--log-level', 'warn'],
            stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic()+30
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError('isolated database exited')
            try:
                with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(f'http://127.0.0.1:{self.port-1}/health', timeout=1) as r:
                    if r.status == 200:
                        return
            except OSError:
                time.sleep(0.1)
        raise TimeoutError('isolated database not ready')

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)

    def close(self):
        self.stop()
        self.log.close()
        self.directory.cleanup()


def scenario(args, mode, sizes, fault):
    name = f'{mode}-{"x".join(map(str, sizes))}-{fault}'
    out = args.out / name
    out.mkdir(parents=True, exist_ok=True)
    proxy = FaultProxy((args.host, args.port))
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen()
    listener.settimeout(15)
    env = dict(os.environ, PROBE_CONTROL_PORT=str(listener.getsockname()[1]),
               PROBE_ENDPOINT=f'127.0.0.1:{proxy.port}', PROBE_MODE=mode,
               PROBE_POOL_SIZES=','.join(map(str, sizes)), PROBE_FILE=str(out / 'file.data'),
               PROBE_BACKEND=f'{args.host}:{args.port}')
    cmd = ['erl', '+S', '4:4', '+SDcpu', '2:2', '+SDio', '4', '-noshell', '-pa', str(ROOT / '_build/regression')]
    cmd += [str(p) for p in sorted((ROOT / '_build/default/lib').glob('*/ebin'))]
    cmd += ['-eval', 'greptimedb_io_probe:main().']
    report = {'name': name, 'mode': mode, 'pool_sizes': sizes, 'fault': fault, 'checks': {}}
    with (out / 'beam.log').open('w') as log:
        beam = subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        sock = None
        try:
            sock, _ = listener.accept()
            control = Control(sock)
            ready = control.read()
            report['ready'] = ready
            if ready['baseline_us'] > 1_000_000:
                raise RuntimeError('invalid environment: baseline I/O exceeded one second')
            proxy.arm()
            fault_start = time.monotonic()
            control.send('go')
            proxy.wait_headers(sum(sizes))
            control.send('measure')
            assert control.read()['event'] == 'measuring'
            time.sleep(1.1)
            report['during_fault'] = control.snapshot()
            file_result = report['during_fault']['file']
            report['checks']['dirty_io_isolation'] = (isinstance(file_result, dict)
                and file_result['result'] == 'ok' and file_result['elapsed_us'] <= 1_000_000)
            healthy = report['during_fault'].get('healthy', {})
            report['checks']['healthy_pool_progress'] = (healthy.get('insert') == 'ok'
                and healthy.get('query') == ['ok', [[7]]] and healthy.get('elapsed_us', 2_000_000) <= 1_000_000)
            time.sleep(max(0, fault_start + 3.1 - time.monotonic()))
            report['deadline_snapshot'] = control.snapshot()
            results = report['deadline_snapshot']['results']
            report['checks']['whole_operation_deadline'] = len(results) == sum(sizes) and all(
                isinstance(r['result'], dict) and 'error' in r['result']
                and r['elapsed_us'] <= 3_000_000 for r in results)
            reclamation_deadline = time.monotonic() + 5
            while True:
                snapshot = control.snapshot()
                resources = snapshot.get('resources')
                if isinstance(resources, dict) and resources.get('operations') == 0:
                    break
                if time.monotonic() >= reclamation_deadline:
                    break
                time.sleep(0.05)
            report['reclamation_snapshot'] = snapshot
            report['checks']['operation_reclamation'] = (
                isinstance(resources, dict) and resources.get('operations') == 0)
            if fault == 'restart':
                proxy.backend_down = True
                args.database.stop()
                proxy.disconnect()
                args.database.start()
                proxy.backend_down = False
            elif fault == 'disconnect':
                proxy.disconnect()
            else:
                proxy.release()
            deadline = time.monotonic() + 12
            while True:
                snapshot = control.snapshot()
                if len(snapshot['results']) == sum(sizes):
                    break
                if time.monotonic() > deadline:
                    raise TimeoutError('operations did not finish after removing fault')
                time.sleep(0.1)
            report['after_release'] = snapshot
            control.send('recover')
            recovery_start = time.monotonic()
            while True:
                snapshot = control.snapshot()
                if 'recovery' in snapshot:
                    break
                if time.monotonic() - recovery_start > 10:
                    raise TimeoutError('new real write/query did not recover within ten seconds')
                time.sleep(0.1)
            if mode.startswith('stream_write'):
                report['stream_recovery'] = snapshot.get('stream_recovery')
                report['checks']['stream_recovery'] = report['stream_recovery'] == {
                    'write': 'ok', 'close': 'ok', 'read': ['ok', [[7]]]}
            report['recovery'] = snapshot['recovery']
            report['checks']['recovery'] = snapshot['recovery'] == {'insert': 'ok', 'query': ['ok', [[7]]]}
            control.send('stop')
            assert control.read()['event'] == 'stopped'
            beam.wait(timeout=5)
            assert beam.returncode == 0
            report['proxy_events'] = proxy.events
            if proxy.errors:
                raise RuntimeError(f'proxy errors: {proxy.errors}')
            report['harness_valid'] = True
        except Exception as exc:
            report['harness_valid'] = False
            report['harness_error'] = repr(exc)
        finally:
            if beam.poll() is None:
                beam.kill()
                beam.wait(timeout=5)
            if sock:
                sock.close()
            listener.close()
            proxy.close()
    report['passed'] = report.get('harness_valid', False) and all(report['checks'].values())
    (out / 'result.json').write_text(json.dumps(report, indent=2))
    print(name, 'PASS' if report['passed'] else 'FAIL', report.get('checks'), report.get('harness_error', ''), flush=True)
    return report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, default=14001)
    p.add_argument('--out', type=Path, default=ROOT / '_build/regression/io')
    p.add_argument('--mode', choices=['query', 'query_async', 'insert', 'insert_async', 'stream_write', 'stream_write_async'])
    p.add_argument('--greptime-bin', help='Start and restart an isolated database owned by this test')
    args = p.parse_args()
    modes = [args.mode] if args.mode else ['query', 'query_async', 'insert', 'insert_async', 'stream_write', 'stream_write_async']
    args.database = Database(args.greptime_bin,args.port) if args.greptime_bin else None
    try:
        if args.database:
            args.database.start()
        results = [scenario(args,mode,sizes,fault) for mode in modes
                   for sizes in ([4],[2,2]) for fault in ('release','disconnect')]
        if args.database:
            results += [scenario(args,mode,[4],'restart') for mode in modes]
        if 'query' in modes:
            results.append(scenario(args,'query',[3],'release'))
    finally:
        if args.database:
            args.database.close()
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / 'summary.json').write_text(json.dumps(results, indent=2))
    valid = all(r['passed'] for r in results)
    return 0 if valid else 1


if __name__ == '__main__':
    raise SystemExit(main())
