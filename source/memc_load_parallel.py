""" Script to load application log files into Memcached """
import threading
import queue

import os
import gzip
import sys
import glob
import logging
import collections
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
# import memcache

from memc_client import memc_set_multi


NORMAL_ERR_RATE = 0.01
CHUNK_SIZE = 10000

AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    """ Rename processed file """
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def pack_appsinstalled(appsinstalled):
    """ Parse line """
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = f"{appsinstalled.dev_type}:{appsinstalled.dev_id}"
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    return {key: packed}


def parse_appsinstalled(line: str):
    """
    Parse app installed log line

    :param line: log line
    :return: parsed app installed log line info, or None if file unprocessable
    """
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`", line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`", line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def worker(options, tasks_queue):
    """ 
    A worker thread:
    - takes a task from task queue 
    - processes it line by line
    - inserts parsed lines into memcached
    """
    address_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    
    buffers_memc = {
        "idfa": {},
        "gaid": {},
        "adid": {},
        "dvid": {},
    }

    thread_name = threading.current_thread().name
    logging.info("%s started", thread_name)

    while True:
        item = tasks_queue.get()
        errors, processed = 0, 0
        for line in item:
            appsinstalled = parse_appsinstalled(line)
            if not appsinstalled:
                errors += 1
                continue
            memc_addr = address_memc.get(appsinstalled.dev_type)
            if not memc_addr:
                errors += 1
                logging.error("Unknow device type: %s", appsinstalled.dev_type)
                continue
            buffers_memc[appsinstalled.dev_type].update(pack_appsinstalled(appsinstalled))
        for key, content in buffers_memc.items():
            processed += len(content)
            logging.info(f'{thread_name} flushing buffer {key} into {address_memc[key]}: {len(content)} items')
            try:
                if options.dry:
                    logging.debug("Address %s - inserted %s values from buffer", address_memc[key], len(content))
                else:
                    keys_errors = memc_set_multi(address_memc[key], content)
                    errors += len(keys_errors)
                    buffers_memc[key].clear()
            except Exception as e:
                logging.exception("Cannot write to memc %s: %s", address_memc[key], e)
                errors += len(content)

        logging.info(f'{thread_name} finished {len(item)} lines, inserted {processed} lines, {errors} errors')
        tasks_queue.task_done()


def gzip_yield_chunks(filename, chunk_size=CHUNK_SIZE):
    """ Read chunk_size lines from gzip file """
    chunk = []
    with gzip.open(filename, 'rt') as fd:
        for line in fd:
            chunk.append(line)
            if len(chunk) == chunk_size:
                yield chunk
                chunk = []
        # yield last chunk
        if chunk:
            yield chunk


def main_parallel(options):
    """ Main function starting threads """
    # Create tasks queue
    tasks_queue = queue.Queue(maxsize=10)
    # Turn-on worker threads
    for t in range(options.workers):
        threading.Thread(target=worker, name=f"Thread {t}" , daemon=True, args=(options, tasks_queue, )).start()

    # Send task requests to the workers
    files = glob.iglob(options.pattern)
    files_processed = 0

    for fn in files:
        logging.info('Processing %s', fn)
        for chunk in gzip_yield_chunks(fn, chunk_size=CHUNK_SIZE):
            tasks_queue.put(chunk)
        dot_rename(fn)
        files_processed += 1
    
    if files_processed:
        logging.info("%s files processed by pattern %s", files_processed, options.pattern)
    else:
        logging.info("Files not found by pattern: %s", options.pattern)

    # Block until all tasks are done.
    tasks_queue.join()
    logging.info('All work completed')


def split_files(options, split_len=200000, output_base='test_'):
    """ Split big file into smaller ones for tests
    @param splitLen - lines per new file
    @param output_base name template for output files
    
    """
    at = 0
    dest = None
    for fn in glob.iglob(options.pattern):
        dir, _ = os.path.split(fn)
        logging.info('Splitting %s' % fn)

        for chunk in gzip_yield_chunks(fn, chunk_size=split_len):
            logging.info('Read %s lines' % len(chunk))
            dest_name = os.path.join(dir, output_base + str(at) + '.tsv.gz')
            dest = gzip.open(dest_name, 'wt')
            for line in chunk:
                dest.write(line)
            dest.close()
            logging.info('Created %s' % dest_name)
            at += 1


def prototest():
    """ Test protocol working """
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-s", "--split", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    # op.add_option("--pattern", action="store", default="./data/appsinstalled/test_2.tsv.gz")
    op.add_option("--pattern", action="store", default="./data/appsinstalled/20240730_1.tsv.gz")
    
    op.add_option("--idfa", action="store", default="127.0.0.1:11211")
    op.add_option("--gaid", action="store", default="127.0.0.1:11211")
    op.add_option("--adid", action="store", default="127.0.0.1:11211")
    op.add_option("--dvid", action="store", default="127.0.0.1:11211")
    op.add_option("-w", "--workers", type=int, action="store", default=4)
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        print("Testing...")
        prototest()
        sys.exit(0)

    if opts.split:
        print("Splitting...")
        split_files(opts, split_len=100000)
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        print("Main proc...")
        main_parallel(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)

