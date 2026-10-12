#!/usr/bin/env python3
"""Real request queue, caller cancellation and pool lifecycle checks."""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import time

from run_io_regression import ROOT, Control, Database, FaultProxy


def scenario(args, name):
    out = args.out/name
    out.mkdir(parents=True, exist_ok=True)
    proxy = FaultProxy(('127.0.0.1', args.port))
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen()
    listener.settimeout(20)
    env = dict(os.environ, PROBE_CONTROL_PORT=str(listener.getsockname()[1]),
               PROBE_ENDPOINT=f'127.0.0.1:{proxy.port}', PROBE_SCENARIO=name,
               PROBE_BACKEND=f'127.0.0.1:{args.port}')
    command = ['erl', '+S', '4:4', '+SDcpu', '2:2', '+SDio', '4', '-noshell', '-pa', str(ROOT/'_build/regression')]
    command += [str(p) for p in sorted((ROOT/'_build/default/lib').glob('*/ebin'))]
    command += ['-eval', 'greptimedb_async_probe:main().']
    report = dict(scenario=name)
    with (out/'beam.log').open('w') as log:
        beam = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        control_socket = None
        try:
            control_socket, _ = listener.accept()
            control = Control(control_socket)
            ready = control.read()
            assert ready['event'] == 'ready'
            assert ready['runtimes'] == 1, 'all pools must share one runtime'
            proxy.arm()
            control.send('go')
            proxy.wait_headers(1)
            control.send('action')
            assert control.read()['event'] == 'action_done'
            if name == 'queued':
                time.sleep(0.35)
                snapshot = control.snapshot()
                queued = [r for r in snapshot['results'] if r['id'] == 'queued']
                assert len(queued) == 1 and queued[0]['result'] == ['error', 'timeout']
                assert queued[0]['elapsed_us'] <= 500000
                assert snapshot['operations'] == 1, 'queued request should expire while first operation remains active'
                report['queued_expiry'] = snapshot
            deadline = time.monotonic()+5
            while True:
                snapshot = control.snapshot()
                expected_runtimes = 0 if name == 'library_stop' else 1
                if snapshot['operations'] == 0 and snapshot['runtimes'] == expected_runtimes:
                    break
                assert time.monotonic() < deadline, 'cancellation or runtime reclamation timed out'
                time.sleep(0.05)
            report['reclaimed_before_release'] = snapshot
            if name == 'queued':
                assert len(snapshot['results']) == 2
                assert all(r['result'] == ['error', 'timeout'] for r in snapshot['results'])
            elif name in ('pool_stop', 'library_stop', 'runtime_owner_exit'):
                assert len(snapshot['results']) == 1 and snapshot['results'][0]['result'] == ['error', 'closed']
            proxy.release()
            if name in ('socket_exit', 'runtime_owner_exit'):
                time.sleep(1.5)
            control.send('recover')
            assert control.read()['event'] == 'recovered'
            time.sleep(0.2)
            final = control.snapshot()
            assert len(final['results']) == len(snapshot['results']), 'duplicate or stale completion'
            report['after_recovery'] = final
            if name == 'pool_stop':
                control.send('cycles')
                cycles = control.read(timeout=30)
                assert cycles['event'] == 'cycles_passed' and cycles['runtime_restarted']
                report['library_restart'] = True
                report['pool_cycles'] = 20
            control.send('stop')
            assert control.read()['event'] == 'stopped'
            beam.wait(timeout=5)
            assert beam.returncode == 0
            assert not proxy.errors, proxy.errors
            report['passed'] = True
        except Exception as error:
            report.update(passed=False, error=repr(error))
        finally:
            if control_socket:
                control_socket.close()
            listener.close()
            proxy.close()
            if beam.poll() is None:
                beam.kill()
                beam.wait()
    (out/'result.json').write_text(json.dumps(report, indent=2))
    print(name, report, flush=True)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--greptime-bin', required=True)
    parser.add_argument('--port', type=int, default=14201)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    database = Database(args.greptime_bin, args.port)
    try:
        database.start()
        results = [scenario(args, name) for name in ('queued', 'caller_cancel', 'pool_stop', 'socket_exit', 'library_stop', 'runtime_owner_exit')]
    finally:
        database.close()
    (args.out/'summary.json').write_text(json.dumps(results, indent=2))
    return 0 if all(r['passed'] for r in results) else 1


if __name__ == '__main__':
    raise SystemExit(main())
