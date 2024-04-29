import os
import re
import pandas as pd
from datetime import datetime
import subprocess

## Experiment Parameters
NUM_RUNS = 10
EXPERIMENT_NAME = None

## Cache Parameters
SHARD = 16
THREAD = 1
ZIPF_SIZE_RATIO = (0.5, 120)
CACHE_SIZE = int(1000000 * ZIPF_SIZE_RATIO[0])
REQUEST_NUM = ZIPF_SIZE_RATIO[1] * 1000000 * (2 * THREAD if THREAD <= 2 else THREAD)
CACHE_TYPES = [
    "LRU_FH",
    "LRU",
    "FIFO_FH",
    "FIFO",
    "LFU_FH",
    "LFU",
]
ZIPF_CONST = 0.99
LATENCY = 5
FH_REBUILD_FREQUENCY = 20

def parse_stats(file_path):
    # Regex patterns for each line
    regex_patterns = {
        'total_time': r"All threads run (?P<total_time>\d+\.\d+) s",
        'avg_stats': r"- (Hit|Other) Avg: (?P<avg>\d+\.\d+) \(stat size: (?P<stat_size>\d+), real size_: (?P<real_size>\d+)\), median: (?P<median>\d+\.\d+), p9999: (?P<p9999>\d+\.\d+), p999: (?P<p999>\d+\.\d+), p99: (?P<p99>\d+\.\d+), p90: (?P<p90>\d+\.\d+)",
        'latency': r"Total Avg Lat: (?P<total_avg_lat>\d+\.\d+) \(size: (?P<size>\d+), miss ratio: (?P<miss_ratio>\d+\.\d+)\)"
    }

    data = {}
    start_parsing = False

    with open(file_path, 'r') as file:
        for line in file:
            if "All threads run" in line:
                match = re.search(regex_patterns['total_time'], line)
                if match:
                    data['Total Run Time'] = float(match.group('total_time'))
                    start_parsing = True
            elif start_parsing:
                for key, regex in regex_patterns.items():
                    if key != 'total_time':
                        match = re.search(regex, line)
                        if match:
                            data.update(match.groupdict())

    return data

if EXPERIMENT_NAME is None:
    ## Default experiment name to current time
    EXPERIMENT_NAME = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

output_directory = 'benchmarks/' + EXPERIMENT_NAME

if not os.path.exists(output_directory):
  os.mkdir(output_directory, exist_ok = True)

results_df = {cache_type: pd.DataFrame() for cache_type in CACHE_TYPES}

for run_num in range(NUM_RUNS):
    for cache_type in CACHE_TYPES:
        freq = FH_REBUILD_FREQUENCY if "FH" in cache_type else 0     
        output_file = f'{output_directory}/{cache_type}{run_num}.txt'
        command = f"./build/test_trace {THREAD} {CACHE_SIZE} {REQUEST_NUM} {SHARD} zipf " \
                  f"{ZIPF_CONST} {cache_type} {LATENCY} {freq} > {output_file}"
        print(f'Starting {command}')
        subprocess.run(command, shell=True)
        result = parse_stats(output_file)
        result_df = pd.DataFrame([result])
        results_df[cache_type] = pd.concat([results_df[cache_type], result_df], ignore_index=True)

for cache_type, df in results_df.items():
    df.to_csv(f'{output_directory}/{cache_type}.csv', index=False)

with open(f'{output_directory}/summary.txt', 'w'):
    for cache_type, df in results_df.items():
        print(f'{cache_type}:\n{df.describe()}\n\n')