#!/usr/bin/env python3
"""Exercise real CPU conversion cancellation, deadlines, bounds and shutdown."""
import argparse
import json
import os
from pathlib import Path
import subprocess

from run_io_regression import Database, ROOT


def scenario(binary, out, mode):
    out.mkdir(parents=True, exist_ok=True)
    database = Database(binary, 14501)
    try:
        database.start()
        env = dict(
            os.environ,
            CPU_SCENARIO=mode,
            CPU_ENDPOINT='127.0.0.1:14501',
            CPU_RESULT=str((out / 'result.json').resolve()),
        )
        command = [
            'erl', '+S', '4:4', '+SDcpu', '2:2', '+SDio', '4',
            '-noshell', '-pa', str(ROOT / '_build/regression'),
        ]
        command += [
            str(p) for p in sorted((ROOT / '_build/default/lib').glob('*/ebin'))
        ]
        command += ['-eval', 'greptimedb_cpu_probe:main().']
        with (out / 'beam.log').open('w') as log:
            process = subprocess.run(
                command, cwd=ROOT, env=env, stdout=log,
                stderr=subprocess.STDOUT, timeout=40,
            )
        if process.returncode:
            return dict(mode=mode, passed=False, exit=process.returncode)
        return json.loads((out / 'result.json').read_text())
    finally:
        database.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--greptime-bin', required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    results = []
    for mode in ('cancel', 'timeout', 'library_stop', 'query_cancel'):
        result = scenario(args.greptime_bin, args.out / mode, mode)
        results.append(result)
        print(mode, result['passed'], flush=True)
    (args.out / 'summary.json').write_text(json.dumps(results, indent=2))
    return 0 if all(r['passed'] is True for r in results) else 1


if __name__ == '__main__':
    raise SystemExit(main())
