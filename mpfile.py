import os
import logging
import hashlib
import threading
import queue
import concurrent.futures
import argparse
import sys


def split_file(filename, file_chunk_size):
    file_size = os.path.getsize(filename)
    chunks = int(file_size / file_chunk_size)
    remainder = file_size % file_chunk_size
    assert file_chunk_size * chunks + remainder == file_size, "FileSize does not match chunk plan"
    # logging.debug("%s: %s %s", filename, chunks, remainder)
    return filename, chunks, remainder


def discover_path_gen(root_path, file_chunk_size):
    if os.path.isfile(root_path):
        yield split_file(root_path, file_chunk_size)
    else:
        for subdir, dirs, path_files in os.walk(root_path):
            for path_file in path_files:
                full_path_file = os.path.join(subdir, path_file)
                yield split_file(full_path_file, file_chunk_size)


def read_chunk(filename, start_chunk):
    logging.debug("\t\tReading file segment %s at %s", os.path.basename(filename), start_chunk)
    with open(filename, "rb") as file:
        file.seek(start_chunk)
        data = file.read(CHUNK_SIZE)
        if logging.getLogger().level == logging.DEBUG:
            hash_segment = hashlib.new(args.algo)
            hash_segment.update(data)
            logging.debug("\t\t%s %s %s", hash_segment.hexdigest(), start_chunk, os.path.basename(filename))
        return data


def stitch_file(data):
    file_hash = hashlib.new(args.algo)
    for entry in data:
        file_hash.update(entry)
    return file_hash.hexdigest()

# processes each file entry in the queue, i.e. reads the file segment, add its to a file queue and calcualtes the hash
# after last entry is read.
def process_entry(filename: str, seek_location: int, sync_access_lock: threading.RLock, file_queue: queue.Queue):
    try:
        chunk = read_chunk(filename, seek_location)
        segment = files[filename]
        logging.debug("\t\t%s in files entry is %s", os.path.basename(filename), segment)
        logging.debug("\t\tSegments processed (BEFORE): %s", files.get(filename))
        # with sync_access:
        files[filename] -= 1
        logging.debug("\t\tSegments processed (AFTER): %s", files.get(filename))
        stitch_data = []
        if files[filename] < 1:
            logging.debug("\t\t\tfile data exist: %s", "YES" if filename in files_data else "False")
            if filename in files_data:
                file_data = files_data[filename]
                logging.debug("\t\t\tInserting segment %s with length %s", seek_location, len(chunk))
                file_data.append([seek_location, chunk])
                segments = len(file_data)
                if segments > 0:
                    logging.debug("\t\t\t\tUNSORTED (%s): %s", segments, [cd[0] for cd in file_data])
                    file_data.sort()
                    logging.debug("\t\t\t\tSORTED (%s): %s", segments, [cd[0] for cd in file_data])
                for chunk_data in file_data:
                    logging.debug("\t\t\t\tUpdating data segment %s (%s)", chunk_data[0], len(chunk_data[1]))
                    stitch_data.append(chunk_data[1])
            else:
                logging.debug("\t\t\t\tUpdating data segment %s (%s)", seek_location, len(chunk))
                stitch_data.append(chunk)
            logging.debug("\t\t\t\t\tCalculate hash: %s, segments: %s", os.path.basename(filename), len(stitch_data))
            calculated_hash = stitch_file(stitch_data)
            # logging.info("%s %s", calculated_hash, filename)
            with sync_access_lock:
                if file_to_write is not None:
                    print(calculated_hash, filename, file=file_to_write)
                else:
                    print(calculated_hash, filename)
            del files[filename]
            del files_data[filename]
        else:
            logging.debug("\t\tAdding %s segment at %s (%s)", os.path.basename(filename), seek_location, len(chunk))
            files_data.setdefault(filename, []).append([seek_location, chunk])
    finally:
        file_queue.task_done()


# consumes the queue by creating various threads.
def consume_queue(files_to_be_processed_queue, executor, sync_access):
    logging.debug("BEGIN Qsize is %s", files_to_be_processed_queue.qsize())
    futures = []
    while not files_to_be_processed_queue.empty() or not all_files_fed.is_set():
        entry = files_to_be_processed_queue.get()
        logging.debug("Reading queue: %s", entry)
        filename = entry[0]
        seek_location = entry[1]
        futures.append(executor.submit(process_entry, filename, seek_location, sync_access, files_to_be_processed_queue))
    logging.debug("END Qsize is %s", files_to_be_processed_queue.qsize())
    logging.debug("Waiting for reads to complete...")
    concurrent.futures.wait(futures)
    # files_to_be_processed_queue.join()
    logging.debug("Processed all files...")


# parse all files amd adds them to a queue. May block if queue is full
def create_queue(path, file_queue):
    for file in discover_path_gen(path, CHUNK_SIZE):
        logging.debug("Enqueueing file: %s", file)
        # with sync_access:
        files[file[0]] = int(file[1]) + 1
        for chunk in (loc*CHUNK_SIZE for loc in range(file[1]) if file[1] > 0):
            logging.debug("\t\tQueueing some chunk: %s - %s, enqueuing [%s]",
                          chunk, chunk+CHUNK_SIZE-1, file_queue.qsize())
            file_queue.put([file[0], chunk, chunk + CHUNK_SIZE - 1])
        logging.debug("\t\tQueueing last chunk: %s (%s), enqueuing at %s", file[1]*CHUNK_SIZE,
                      file[2], file_queue.qsize())
        file_queue.put([file[0], file[1] * CHUNK_SIZE, file[1] * CHUNK_SIZE + file[2]])
    logging.debug("Enqueued all files...")
    all_files_fed.set()


# main entry point to start producer and consumer threads
def main(files_location):
    sync_queue = queue.Queue(QUEUE_SIZE)
    sync_access = threading.RLock()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=CORES, thread_name_prefix="mpfile")
    executor.submit(create_queue, files_location, sync_queue)
    consume_queue(sync_queue, executor, sync_access)


# Parse all arguments with defaults for number of threads and hash algorithm to choose.
def parse_args():
    cpu_count = os.cpu_count()
    threads = cpu_count if cpu_count > 1 else 2
    parser = argparse.ArgumentParser(description="Generate fast check sums.")
    parser.add_argument('-algo', metavar='-a', choices=sorted(hashlib.algorithms_guaranteed), default="sha256",
                        help="select default hashing algorithm.")
    parser.add_argument('-threads', metavar='-t', type=int, choices=[threads, threads*2, threads*4, threads*8],
                        default=threads, help="Number of parallel file reads to perform.")
    parser.add_argument('-version', metavar='-v', help="Show version number.")
    parser.add_argument('-path', metavar='-p', help="file or root directory", required=True)
    parser.add_argument('-s', action='store_true', help="Suppress config output")
    parser.add_argument('-file', metavar='-f', help="Store output in a file")
    return parser.parse_args()


if __name__ == '__main__':
    # set standard error logging format
    logging.basicConfig(format='[%(threadName)s] %(asctime)s %(message)s', level=logging.ERROR)
    args = parse_args() # read args passed by user
    file_to_write = args.file
    location = args.path
    logging.debug("Ready with %s", args)
    try:
        if os.path.exists(location):
            CORES = args.threads
            QUEUE_SIZE = CORES * 10
            CHUNK_SIZE = 1024 * 1024 * 10  # 10M
            files = {}
            files_data = {}
            all_files_fed = threading.Event()
            if args.s is False:
                print("Using version=1.0, threads:", CORES, ", algorithm=", args.algo)
            if file_to_write is not None:
                with open(file_to_write, "w") as file_to_write:
                    main(location)
            else:
                main(location)
        else:
            raise FileNotFoundError(location)
    except OSError as fnf:
        print("Error processing: ", fnf, file=sys.stderr)
