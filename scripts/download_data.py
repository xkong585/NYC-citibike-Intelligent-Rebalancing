"""Fetch and verify the exact January 2023 archive used by the local reproduction."""
from pathlib import Path
import argparse, hashlib, urllib.request

URL='https://media.githubusercontent.com/media/sunaminusone/NYC-citibike-Intelligent-Rebalancing/main/data/202301-citibike-tripdata.zip'
SHA256='7a3a25d977e72f46307d52cab8a450ceb0f496533c6e9cabfd4bc3c4655c8849'

def main():
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,default=Path('data/raw/202301-citibike-tripdata.zip'));args=p.parse_args()
    args.output.parent.mkdir(parents=True,exist_ok=True)
    if not args.output.exists():
        partial=args.output.with_suffix('.download')
        with urllib.request.urlopen(URL,timeout=60) as source,partial.open('wb') as target:
            while chunk:=source.read(1024*1024):target.write(chunk)
        partial.replace(args.output)
    h=hashlib.sha256()
    with args.output.open('rb') as f:
        while chunk:=f.read(1024*1024):h.update(chunk)
    if h.hexdigest()!=SHA256:raise ValueError('Archive checksum differs from the source snapshot; do not run this reproduction on it.')
    print(f'Verified {args.output} ({args.output.stat().st_size:,} bytes)')

if __name__=='__main__':main()
