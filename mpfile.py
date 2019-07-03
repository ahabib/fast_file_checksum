import os
import sys
import logging
import hashlib
import threading
import queue
import concurrent.futures
import time


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
    logging.debug("Reading %s at %s", filename, start_chunk)
    with open(filename, "rb") as file:
        file.seek(start_chunk)
        data = file.read(CHUNK_SIZE)
        if logging.getLogger().level == logging.DEBUG:
            hash_segment = hashlib.sha256()
            hash_segment.update(data)
            logging.debug("\t\t%s %s %s", hash_segment.hexdigest(), start_chunk, filename)
        return data


def process_entry(filename, seek_location):
    try:
        chunk = read_chunk(filename, seek_location)
        segment = files[filename]
        logging.debug("%s in files entry is %s", filename, segment)
        files[filename] -= 1
        if files[filename] < 0:
            logging.debug("file data exist: %s", "YES" if filename in files_data else "False")
            hash_func = hashlib.sha256()
            if filename in files_data:
                file_data = files_data[filename]
                segments = len(file_data)
                if segments > 1:
                    logging.debug("UNSORTED (%s): %s", segments, [cd[0] for cd in file_data])
                    file_data.sort()
                    logging.debug("SORTED (%s): %s", segments, [cd[0] for cd in file_data])
                for chunk_data in file_data:
                    hash_func.update(chunk_data[1])
            hash_func.update(chunk)
            calculated_hash = hash_func.hexdigest()
            logging.info("%s %s", calculated_hash, filename)
            # print(calculated_hash, filename)
            # del files[filename]
            # del files_data[filename]
        else:
            files_data.setdefault(filename, []).append([seek_location, chunk])
    finally:
        sync_queue.task_done()


def consume_queue(files_to_be_processed_queue, executor):
    logging.debug("BEGIN Qsize is %s", files_to_be_processed_queue.qsize())
    futures = []
    while not files_to_be_processed_queue.empty() or not all_files_fed.is_set():
        entry = files_to_be_processed_queue.get()
        logging.debug("Read queue: %s", entry)
        filename = entry[0]
        seek_location = entry[1]
        futures.append(executor.submit(process_entry, filename, seek_location))
    logging.debug("END Qsize is %s", files_to_be_processed_queue.qsize())
    logging.debug("Waiting for reads to complete...")
    concurrent.futures.wait(futures)
    logging.debug("Reading complete...")


# may block if queue is full
def create_queue(path, file_queue):
    for file in discover_path_gen(path, CHUNK_SIZE):
        logging.debug("Enqueueing file: %s", file)
        files[file[0]] = int(file[1])
        for chunk in (loc*CHUNK_SIZE for loc in range(file[1]) if file[1] > 0):
            logging.debug("\t\tQueueing some chunk: %s - %s, enqueuing [%s]",
                          chunk, chunk+CHUNK_SIZE-1, file_queue.qsize())
            file_queue.put([file[0], chunk, chunk + CHUNK_SIZE - 1])
        logging.debug("\t\tQueueing last chunk: %s (%s), enqueuing at %s", file[1]*CHUNK_SIZE,
                      file[2], file_queue.qsize())
        file_queue.put([file[0], file[1] * CHUNK_SIZE, file[1] * CHUNK_SIZE + file[2]])
    all_files_fed.set()
    logging.debug("Enqueued everything...waiting for all threads to be processed.")


def main(files_location, read_queue):
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=CORES, thread_name_prefix="mpfile")
    executor.submit(create_queue, files_location, read_queue)
    consume_queue(read_queue, executor)


if __name__ == '__main__':
    try:
        logging.basicConfig(format='[%(threadName)s] %(asctime)s %(message)s', level=logging.DEBUG)
        CORES = os.cpu_count()
        QUEUE_SIZE = CORES
        CHUNK_SIZE = 1024 * 1024 * 2 # 1M
        files = {}
        files_data = {}
        location = sys.argv[1] if len(sys.argv) > 1 and os.path.exists(sys.argv[1]) else None
        all_files_fed = threading.Event()
        sync_queue = queue.Queue(QUEUE_SIZE)
        if location is not None:
            main(location, sync_queue)
        else:
            logging.error("Usage: %s /path", os.path.basename(sys.argv[0]))
    except KeyboardInterrupt:
        logging.error("User interrupted")
