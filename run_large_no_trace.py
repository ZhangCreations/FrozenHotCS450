import os
import re
import argparse
import subprocess
import pandas as pd
from datetime import datetime

# Constants
REGEX_PATTERNS = [
    re.compile(r"All threads run (?P<run_time>[\d.]+) s"),
    re.compile(r"- Hit Avg: (?P<hit_avg>[\d.]+) \(stat size: (?P<hit_stat_size>\d+), real size_: (?P<hit_real_size>\d+)\), median: (?P<hit_median>[\d.]+), p9999: (?P<hit_p9999>[\d.]+), p999: (?P<hit_p999>[\d.]+), p99: (?P<hit_p99>[\d.]+), p90: (?P<hit_p90>[\d.]+)"),
    re.compile(r"- Other Avg: (?P<other_avg>[\d.]+) \(stat size: (?P<other_stat_size>\d+), real size_: (?P<other_real_size>\d+)\), median: (?P<other_median>[\d.]+), p9999: (?P<other_p9999>[\d.]+), p999: (?P<other_p999>[\d.]+), p99: (?P<other_p99>[\d.]+), p90: (?P<other_p90>[\d.]+)"),
    re.compile(r"Total Avg Lat: (?P<total_avg_lat>[\d.]+) \(size: (?P<total_size>\d+), miss ratio: (?P<miss_ratio>[\d.]+)\)")
]
CACHE_TYPES = ["FIFO_FH", "LFU_FH", "LRU_FH", "FIFO", "LFU", "LRU"]

# Configurable parameters
thread = 1
shard = 16
latency = 5
frequency = 20
thread_multiplier = 2
zipf_const = 0.99
ratio = 0.5
num_requests = 25000000
cache_size = int(num_requests * ratio)

# Argument parser
def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description='Testing Harness for FrozenHot Cache')
    parser.add_argument('-r', '--num_runs', type=int, help='Number of runs for the experiment', required=True)
    parser.add_argument('-n', '--experiment_name', type=str, help='Name of the experiment', default=datetime.now().strftime("%Y-%m-%d_%H:%M:%S"))
    return parser.parse_args()

# Running the experiment
def run_experiment(num_runs, output_directory):
    agg_data = {cache_type: pd.DataFrame() for cache_type in CACHE_TYPES}
    for run_num in range(num_runs):
        destination_dir = f'{output_directory}/run_{run_num}'
        os.makedirs(destination_dir, exist_ok=True)
        for cache_type in CACHE_TYPES:
            run_data = {}
            run_output_file = f'{destination_dir}/{cache_type}.txt'
            command = f"./build/test_trace {thread} {cache_size} {num_requests} {shard} Zipf {zipf_const} {cache_type} {latency} {frequency} > {run_output_file}"
            print(command)
            try:
                subprocess.run(command, check=True, shell=True)
            except subprocess.CalledProcessError as e:
                print(f"Command failed with error: {e}")
                continue

            with open(run_output_file, 'r') as file:
                lines = file.readlines()
                for i, line in enumerate(lines):
                    if line.startswith("All threads run"):
                        for j in range(4):
                            run_data.update({key: float(value) for key, value in REGEX_PATTERNS[j].search(lines[i + j]).groupdict().items()})
                        break
            agg_data[cache_type] = agg_data[cache_type].append(run_data, ignore_index=True)
    summarize_results(agg_data)

def summarize_results(agg_data, output_directory):
    output_dir = f"{output_directory}/summaries"
    output_file = f"{output_dir}/results.txt"
    os.makedirs(output_dir, exist_ok=True)

    with open(output_file, 'w') as f:
        for cache_type, df in agg_data.items():
            output_csv = f"{output_dir}/{cache_type}.csv"
            try:
                df.to_csv(output_csv, index=False)
            except Exception as e:
                print(f"Failed to write {cache_type} to {output_csv} due to {e}")
            f.write(f"Cache type: {cache_type}\n\n")
            quantiles = df.quantile([0.25, 0.5, 0.75]).to_string(index=False, float_format="%.5f")
            mean_results = df.mean().to_string(float_format="%.5f")

            f.write(f"Cache type: {cache_type}\n\n")
            f.write("Quantiles:\n" + quantiles + "\n\n")
            f.write("Means:\n" + mean_results + "\n")
            f.write("==========================================\n\n")

if __name__ == "__main__":
    args = parse_args()
    output_directory = f'benchmarks/{args.experiment_name}'
    os.makedirs(output_directory, exist_ok=True)
    run_experiment(args.num_runs, output_directory)
    print("Done!")