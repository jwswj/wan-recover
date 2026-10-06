#!/usr/bin/env python3
"""Install the read-only health helper using an authenticated SSH master.

Requires an independently verified host-key file. Does not read administrator
passwords, alter WAN settings, or write credentials to a cloud service.
"""
import argparse
import ipaddress
import os
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--control-path', required=True)
parser.add_argument('--gateway', required=True, help='gateway private IPv4 address')
parser.add_argument('--node', required=True, help='private IPv4 source address of the recovery service')
parser.add_argument('--secrets-dir', type=Path, default=Path('secrets'))
args = parser.parse_args()
for address in (args.gateway, args.node):
    value = ipaddress.ip_address(address)
    if value.version != 4 or not value.is_private:
        parser.error('gateway and node must have private IPv4 addresses')
root = Path(__file__).resolve().parent.parent
private = args.secrets_dir.resolve()
private.mkdir(mode=0o700, exist_ok=True)
key = private / 'gateway-key'
known_hosts = private / 'known_hosts'
if not known_hosts.is_file():
    parser.error('secrets directory must contain independently verified known_hosts')
ssh = ['ssh', '-S', args.control_path, '-o', 'BatchMode=yes',
       '-o', 'StrictHostKeyChecking=yes', '-o', 'UserKnownHostsFile=' + str(known_hosts),
       'root@' + args.gateway]
subprocess.run(ssh[:-1] + ['-O', 'check', ssh[-1]], check=True, capture_output=True)
if not key.exists():
    subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '',
                    '-C', 'wan-recover-health', '-f', str(key)], check=True)
os.chmod(key, 0o600)
subprocess.run(ssh + ['umask 077; mkdir -p /data/wan-recover; '
                     'cat > /data/wan-recover/wan-health.py; '
                     'chmod 700 /data/wan-recover/wan-health.py'],
               input=(root / 'gateway/wan-health.py').read_bytes(), check=True)
public = key.with_suffix('.pub').read_text().strip()
line = (f'from="{args.node}",restrict,command="/usr/bin/python3 '
        '/data/wan-recover/wan-health.py" ' + public)
program = '''from pathlib import Path
p = Path('/root/.ssh/authorized_keys')
p.parent.mkdir(mode=0o700, exist_ok=True)
lines = p.read_text().splitlines() if p.exists() else []
line = LINE
key = KEY
matches = [v for v in lines if key in v]
if matches and matches != [line]:
    raise SystemExit('Key already installed with different restrictions; inspect manually')
if not matches:
    with p.open('a') as f:
        f.write(('\\n' if lines else '') + line + '\\n')
    p.chmod(0o600)
'''.replace('LINE', repr(line)).replace('KEY', repr(public.split()[1]))
subprocess.run(ssh + ['/usr/bin/python3 -'], input=program.encode(), check=True)
print('Installed source-restricted read-only gateway helper. Keep the private key out of Git.')
