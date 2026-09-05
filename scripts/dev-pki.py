#!/usr/bin/env python3
"""Create a disposable development CA and mTLS identities. Never overwrites."""
import argparse
import hashlib
import os
from pathlib import Path
import re
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("directory", type=Path)
parser.add_argument("--server-name", default="localhost")
args = parser.parse_args()
if not re.fullmatch(r"[A-Za-z0-9.-]+", args.server_name):
    parser.error("server name must be a DNS name")
args.directory.mkdir(mode=0o700, parents=False, exist_ok=False)
root = args.directory.resolve()
def run(*argv):
    subprocess.run(["openssl", *argv], cwd=root, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
run("req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes", "-keyout", "ca.key", "-out", "ca.pem", "-days", "30", "-subj", "/CN=Matrix development CA", "-addext", "basicConstraints=critical,CA:TRUE")
for name in ("server", "client"):
    run("req", "-new", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes", "-keyout", f"{name}.key", "-out", f"{name}.csr", "-subj", f"/CN={name}")
    extension = f"subjectAltName=DNS:{args.server_name}\nextendedKeyUsage=serverAuth\n" if name == "server" else "extendedKeyUsage=clientAuth\n"
    (root / "extension.cnf").write_text(extension + "basicConstraints=CA:FALSE\n")
    run("x509", "-req", "-in", f"{name}.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial", "-out", f"{name}.pem", "-days", "30", "-extfile", "extension.cnf")
    run("x509", "-in", f"{name}.pem", "-outform", "DER", "-out", f"{name}.der")
    run("pkcs8", "-topk8", "-nocrypt", "-in", f"{name}.key", "-outform", "DER", "-out", f"{name}-key.der")
run("x509", "-in", "ca.pem", "-outform", "DER", "-out", "ca.der")
for file in root.iterdir():
    if file.is_file():
        os.chmod(file, 0o600)
print("client fingerprint:", hashlib.sha256((root / "client.der").read_bytes()).hexdigest())
print("development PKI:", root)
