import os
import logging
import hashlib
import asyncio
import threading


def split_file(filename, file_chunk_size):
    file_size = os.path.getsize(filename)
    chunks = int(file_size / file_chunk_size)
    remainder = file_size % file_chunk_size
    assert file_chunk_size * chunks + remainder == file_size, "FileSize does not match chunk plan"
    # logging.debug("%s: %s %s", filename, chunks, remainder)
    return filename, chunks, remainder


def discover_path_gen(root_path, file_chunk_size):
    for subdir, dirs, path_files in os.walk(root_path):
        for path_file in path_files:
            full_path_file = os.path.join(subdir, path_file)
            yield split_file(full_path_file, file_chunk_size)


def read_chunk(filename, start_chunk):
    logging.debug("Reading %s at %s", filename, start_chunk)
    with open(filename, "rb") as file:
        file.seek(start_chunk)
        return file.read(CHUNK_SIZE)


def process_entry(filename, seek_location):
    chunk = read_chunk(filename, seek_location)
    segment = files[filename]
    logging.debug("%s in files entry is %s", filename, segment)
    files[filename] -= 1
    if files[filename] < 0:
        logging.debug("file data exist: %s", "YES" if filename in files_data  else "False")
        hash_func = hashlib.sha256()
        if filename in files_data:
            file_data = files_data[filename]
            logging.debug("UNSORTED: %s", [cd[0] for cd in file_data])
            file_data.sort()
            logging.debug("SORTED: %s", [cd[0] for cd in file_data])
            for chunk_data in file_data:
                hash_func.update(chunk_data[1])
        hash_func.update(chunk)
        calculated_hash = hash_func.hexdigest()
        logging.info("%s %s", calculated_hash, filename)
    else:
        files_data.setdefault(filename, []).append([seek_location, chunk])


# may block if queue is full
async def create_queue(path, file_queue):
    with done_with_files:
        for file in discover_path_gen(path, CHUNK_SIZE):
            logging.debug("Processing file: %s", file)
            files[file[0]] = int(file[1])
            for chunk in (location*CHUNK_SIZE for location in range(file[1]) if file[1] > 0):
                logging.debug("\tQueueing some chunk: %s - %s, enqueuing [%s]",
                              chunk, chunk+CHUNK_SIZE-1, file_queue.qsize())
                await file_queue.put([file[0], chunk, chunk + CHUNK_SIZE - 1])
            logging.debug("\tQueueing last chunk: %s - %s, enqueuing [%s]", file[1]*CHUNK_SIZE, file[1]*CHUNK_SIZE + file[2], file_queue.qsize())
            await file_queue.put([file[0], file[1] * CHUNK_SIZE, file[1] * CHUNK_SIZE + file[2]])


async def consume_queue(files_to_be_processed_queue):
    logging.debug("Queue size is %s", files_to_be_processed_queue.qsize())
    while not files_to_be_processed_queue.empty():
        entry = (await files_to_be_processed_queue.get())
        logging.debug("Read queue: %s", entry)
        filename = entry[0]
        seek_location = entry[1]
        # logging.debug("Reading %s [%s]", filename, seek_location)
        process_entry(filename, seek_location)
        files_to_be_processed_queue.task_done()


async def main():
    await create_queue("/Users/adnan/Downloads/Shaiyaan", read_queue)
    await consume_queue(read_queue)


CORES = os.cpu_count()
QUEUE_SIZE = CORES * 30
logging.basicConfig(datefmt='[%Y/%m/%d %H:%M:%S]', format='%(asctime)s %(message)s', level=logging.INFO)
CHUNK_SIZE = 2097152  # 2M
read_queue = asyncio.Queue(QUEUE_SIZE)
files = {}
files_data = {}
done_with_files = threading.Condition()
asyncio.run(main())
