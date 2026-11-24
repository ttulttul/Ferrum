#!/usr/bin/env python3
"""A stand-in for `ssh` that never leaves this machine, for the SSH device host suites.

Hosts come from the JSON file in $T3_FAKE_SSH_HOSTS, keyed by the target the server passes:
  {"<target>": {"home": "<remote $HOME>", "hostname": "<what -G reports>",
                "fail": "resolve" | "connect" | "auth" | "host_key", "windows": true}}
A host's remote command runs here, as `sh -c '<words joined>'`, with a fresh environment whose
HOME is the host's home (as a login over SSH would have), and the ssh's stdin. `-L` forwards a
loopback port to the "remote" loopback port, which is this machine's. Every call is appended to
$T3_FAKE_SSH_LOG: its arguments, and its stdin's owner line and digest (the bootstrap script).
"""
import hashlib
import json
import os
import re
import socket
import subprocess
import sys
import threading

VALUED = {"-o", "-p", "-i", "-L", "-R", "-D", "-F", "-l", "-E", "-J", "-b", "-c", "-e", "-m", "-O", "-Q", "-S", "-W", "-w", "-B", "-I"}


def parse(argv):
    options, index = [], 0
    while index < len(argv):
        arg = argv[index]
        if arg == "--":
            index += 1
            break
        if not arg.startswith("-") or arg == "-":
            break
        if arg in VALUED:
            options.append((arg, argv[index + 1]))
            index += 2
        else:
            options.append((arg, None))
            index += 1
    host = argv[index] if index < len(argv) else None
    return options, host, argv[index + 1:]


def forward(listen_port, target_port):
    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", listen_port))
    server.listen(16)

    def pipe(source, sink):
        try:
            while chunk := source.recv(65536):
                sink.sendall(chunk)
        except OSError:
            pass
        finally:
            for end in (source, sink):
                try:
                    end.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def accept():
        while True:
            client, _ = server.accept()
            try:
                upstream = socket.create_connection(("127.0.0.1", target_port))
            except OSError:
                client.close()
                continue
            threading.Thread(target=pipe, args=(client, upstream), daemon=True).start()
            threading.Thread(target=pipe, args=(upstream, client), daemon=True).start()

    threading.Thread(target=accept, daemon=True).start()


def main():
    argv = sys.argv[1:]
    options, host, words = parse(argv)
    hosts = json.loads(open(os.environ["T3_FAKE_SSH_HOSTS"]).read())
    tunnel = any(option == "-L" for option, _ in options)
    stdin = b"" if tunnel or sys.stdin.isatty() else sys.stdin.buffer.read()
    entry = {"args": argv}
    if stdin:
        text = stdin.decode()
        owner = re.search(r'^const owner = "([^"]*)";$', text, re.M)
        mode = re.search(r'^const mode = "([^"]*)";$', text, re.M)
        if owner:
            entry["owner"] = owner.group(1)
            text = text.replace(owner.group(0), 'const owner = "<owner>";')
        if mode:
            entry["mode"] = mode.group(1)
        entry["stdin"] = hashlib.sha256(text.encode()).hexdigest()[:16]
    with open(os.environ["T3_FAKE_SSH_LOG"], "a") as log:
        log.write(json.dumps(entry) + "\n")
    config = hosts.get(host or "", {})
    if ("-G", None) in options:
        port = next((value for option, value in options if option == "-p"), "22")
        print(f"hostname {config.get('hostname', '192.0.2.10')}\nport {port}\nproxycommand none\nproxyjump none")
        return 0
    failure = config.get("fail")
    if failure is None and host not in hosts:
        failure = "resolve"
    if failure == "resolve":
        sys.stderr.write(f"ssh: Could not resolve hostname {host}: Name or service not known\r\n")
        return 255
    if failure == "connect":
        sys.stderr.write(f"ssh: connect to host {host} port 22: Connection refused\r\n")
        return 255
    if failure == "auth":
        sys.stderr.write(f"tester@{host}: Permission denied (publickey).\r\n")
        return 255
    if failure == "host_key":
        sys.stderr.write("Host key verification failed.\r\n")
        return 255
    command = " ".join(words)
    if config.get("windows"):
        if command == "echo %OS% $env:OS":
            print("Windows_NT $env:OS")
            return 0
        sys.stderr.write("'sh' is not recognized as an internal or external command,\r\noperable program or batch file.\r\n")
        return 1
    forwards = [value for option, value in options if option == "-L"]
    for spec in forwards:
        _, listen, _, target = spec.split(":")
        forward(int(listen), int(target))
    home = config["home"]
    env = {"HOME": home, "USER": "tester", "LOGNAME": "tester", "SHELL": "/bin/sh",
           "PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}
    if forwards:
        # The tunnel's remote `cat` holds the connection until the server closes its stdin.
        done = subprocess.run(["/bin/sh", "-c", command], stdin=sys.stdin, env=env, cwd=home)
    else:
        done = subprocess.run(["/bin/sh", "-c", command], input=stdin, env=env, cwd=home)
    return done.returncode


if __name__ == "__main__":
    sys.exit(main())
