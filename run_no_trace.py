import os
import re
import pandas as pd
from datetime import datetime
import subprocess
import argparse

## Cache Parameters
SHARD = 16
THREAD = 1
ZIPF_SIZE_RATIO = (0.5, 120)
CACHE_SIZE = int(1000000 * ZIPF_SIZE_RATIO[0])
REQUEST_NUM = ZIPF_SIZE_RATIO[1] * 1000000 * (2 * THREAD if THREAD <= 2 else THREAD)
CACHE_TYPES = [
    # "LRU_FH",
    "LRU",
    "FIFO_FH",
    "FIFO",
    "LFU_FH",
    "LFU",
]
ZIPF_CONST = 0.99
LATENCY = 5
FH_REBUILD_FREQUENCY = 20

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description='Testing Harness for FrozenHot Cache')
    parser.add_argument('-r', '--num_runs', type=int, help='Number of runs for the experiment', required=True)
    parser.add_argument('-n', '--experiment_name', type=str, help='Name of the experiment', default=datetime.now().strftime("%Y-%m-%d_%H:%M:%S"))
    return parser.parse_args()

def parse_stats(file_path):
    """Parse statistical data from the given file."""
    regex_patterns = {
        'total_time': re.compile(r"All threads run (?P<total_time>\d+\.\d+) s"),
        'hit_avg_stats': re.compile(r"- Hit Avg: (?P<hit_avg>\d+\.\d+) \(stat size: (?P<hit_stat_size>\d+), real size_: (?P<hit_real_size>\d+)\), median: (?P<hit_median>\d+\.\d+), p9999: (?P<hit_p9999>\d+\.\d+), p999: (?P<hit_p999>\d+\.\d+), p99: (?P<hit_p99>\d+\.\d+), p90: (?P<hit_p90>\d+\.\d+)"),
        'other_avg_stats': re.compile(r"- Other Avg: (?P<other_avg>\d+\.\d+) \(stat size: (?P<other_stat_size>\d+), real size_: (?P<other_real_size>\d+)\), median: (?P<other_median>\d+\.\d+), p9999: (?P<other_p9999>\d+\.\d+), p999: (?P<other_p999>\d+\.\d+), p99: (?P<other_p99>\d+\.\d+), p90: (?P<other_p90>\d+\.\d+)"),
        'latency': re.compile(r"Total Avg Lat: (?P<total_avg_lat>\d+\.\d+) \(size: (?P<size>\d+), miss ratio: (?P<miss_ratio>\d+\.\d+)\)")
    }
    
    data = {}
    with open(file_path, 'r') as file:
        lines = file.readlines()

    for i, line in enumerate(lines):
        if line.startswith("All threads run"):
            data.update(regex_patterns['total_time'].search(lines[i]).groupdict())
            data.update(regex_patterns['hit_avg_stats'].search(lines[i + 1]).groupdict())
            data.update(regex_patterns['other_avg_stats'].search(lines[i + 2]).groupdict())
            data.update(regex_patterns['latency'].search(lines[i + 3]).groupdict())
            break

    return data


def run_experiment(num_runs, output_directory):
    """Run the experiment for given number of runs and save the results."""
    

    results_df = {cache_type: pd.DataFrame() for cache_type in CACHE_TYPES}

    for run_num in range(num_runs):
        for cache_type in CACHE_TYPES:
            freq = FH_REBUILD_FREQUENCY if "FH" in cache_type else 0
            output_file = f'{output_directory}/{cache_type}{run_num}.txt'
            command = f"./build/test_trace {THREAD} {CACHE_SIZE} {REQUEST_NUM} {SHARD} Zipf {ZIPF_CONST} {cache_type} {LATENCY} {freq} > {output_file}"
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

if __name__ == "__main__":
    args = parse_args()
    output_directory = f'benchmarks/{args.experiment_name}'
    os.makedirs(output_directory, exist_ok=True)
    run_experiment(args.num_runs, output_directory)
    print("Done!")