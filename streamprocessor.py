import time
import weakref
import multiprocessing


class StreamProcessor:
    def __init__(self, input_stream, map_function, reduce_generator=None, *,
                 number_of_processes=multiprocessing.cpu_count()):
        self.input_stream = input_stream
        self.map_function = map_function
        self.reduce_generator = reduce_generator
        self.number_of_processes = number_of_processes

    def __iter__(self):
        with multiprocessing.Pool(processes=self.number_of_processes) as pool:
            output_stream = pool.map(self.map_function, self.input_stream)
            if self.reduce_generator:
                output_stream = self.reduce_generator(iter(output_stream))
            for item in output_stream:
                yield item


def is_prime(n):
    time.sleep(1)

    if n < 2:
        return False, n
    elif n == 2:
        return True, n
    sqrt_n = int(n**0.5)+1
    return len([i for i in range(2, sqrt_n+1) if n % i == 0]) == 0, n


def only_primes(stream):
    try:
        while True:
            is_valid, value = next(stream)
            while not is_valid:
                is_valid, value = next(stream)
            yield value
    except StopIteration:
        return


def streaming_statistics(stream):
    total = 0
    n = 0
    for item in stream:
        total += item
        n += 1
        mean = total / n
        yield {'mean': mean}
