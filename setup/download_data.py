import os
import time


import sys; sys.path.append('..')
import argparse


import EDGAR

# Command line magic for common use case to regenerate dataset
#   --force to overwrite outdated local files
parser = argparse.ArgumentParser()
parser.add_argument('-f', '--force', action='store_true')
args = parser.parse_args()

loader = EDGAR.downloader(data_dir=os.path.join('..','data'));

# List of companies to process
tikrs = open(os.path.join(loader.path, os.path.join('..','tickers.txt'))).read().strip()
tikrs = [i.split(',')[0].lower() for i in tikrs.split('\n')]

for tikr in tikrs:
    loader.metadata.load_tikr_metadata(tikr)

# Check if some are already downloaded, do not redownload
to_download = [];
for tikr in tikrs:
    if not loader._is_downloaded(tikr) or args.force:
        to_download.append(tikr)

# Download missing files
if len(to_download) != 0:
    if args.force:
        print(f"Forcing downnloads...")
    else:
        print(f"Downloaded: {str(list(set(tikrs) - set(to_download)))}")
    print(f"Downloading... {str(to_download)}")
    for tikr in to_download:
        loader.query_server(tikr)
        time.sleep(5)
else:
    print('Everything on Ticker List already downloaded.')

# Unpack downloaded files into relevant directories
to_unpack = []
for tikr in tikrs:
    if not loader._is_10q_unpacked(tikr) or args.force:
        to_unpack.append(tikr)

if len(to_unpack) != 0:
    if args.force:
        print(f"Forcing Unpacking...")
    else:
        print(f"Unpacked: {str(list(set(tikrs) - set(to_unpack)))}")
    print(f"Unpacking... {str(to_unpack)}")
    for tikr in to_unpack:
        loader.unpack_bulk(tikr, loading_bar=True, desc=f"{tikr} :Inflating HTM")
else:
    print('All downloaded 10-Q files already unpacked')
