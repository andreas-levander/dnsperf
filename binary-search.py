from subprocess import run, PIPE
import argparse
import configparser
import datetime
import os
import shutil
import math

"""
dnsperfconfig = {
    'server': '172.18.0.2',
    'port': '53',
    'subnet': '10.0.0.0/8',
    'requests': '16',
    'burst_size': '8',
    'threads': '1',
    'ports_per_thread': '1',
    'burst_delay': '1000', # ns
    'timeout': '1', # seconds
}
"""
def calculate_max_requests(config):
    subnet = int(config['subnet'].split('/')[1])
    return 2 ** (32 - subnet)


def calculate_qps(burst_size, burst_delay):
    return math.ceil((burst_size / burst_delay) * 1e9)

def set_qps(cfg, qps, delay, runtime, max_requests):
    burst_size = (qps * delay) / 1e9
    if burst_size < 1:
        #print("Burst size too small, increasing delay by x10")
        return set_qps(cfg, qps, delay * 10, runtime, max_requests)
    burst_size = math.ceil(burst_size)
    cfg['burst_size'] = str(burst_size)
    # adjust delay to get qps
    delay = math.ceil((burst_size / qps) * 1e9)
    cfg['burst_delay'] = str(delay)
    threads = int(cfg['threads'])
    real_qps = calculate_qps(burst_size, delay) * threads
    requests = real_qps * runtime 

    div = burst_size * threads
    while requests % div != 0:
            #print("Requests not divisible by burst size * threads, increasing requests")
            requests += 1
    if requests > max_requests:
        raise Exception(f"Too many requests {requests:,} > MAX ALLOWED {max_requests:,}")
    cfg['requests'] = str(requests)
    print(f"Setting QPS/t to {qps:,} - rQPS {real_qps:,} with burst size {burst_size:,} and burst delay {delay:,}ns with {requests:,} requests")
    return real_qps

def run_dnsperf(cfg):
    print("Running dns64perf++")
    res = run(["dns64perf++", cfg['server'], cfg['port'], cfg['subnet'], cfg['requests'], cfg['burst_size'], cfg['threads'],
               cfg['ports_per_thread'], cfg['burst_delay'], cfg['timeout']], stdout=PIPE, check=True)
    print(res.stdout.decode("utf-8"))
    return res.stdout.decode("utf-8")

def parse_dnsperf_output(output):
    lines = output.split("\n")
    for line in lines:
        if "Sent queries" in line:
            queries = int(line.split(":")[1].strip())
        elif "Received answers" in line:
            answers = int(line.split(":")[1].strip().split(" ")[0].strip())
        elif "Valid answers" in line:
            valid = int(line.split(":")[1].strip().split(" ")[0].strip())
    return queries, answers, valid

def binary_searchQPS(dnsperfconfig, low, high, runtime, accuracy, max_requests, log_target, delay):
    while low < high:
        mid = low + math.ceil((high - low) / 2)
        print(f"Low: {low:,}, High: {high:,}, Mid: {mid:,}")
        if high - low < accuracy:
            break
        rqps = set_qps(dnsperfconfig, mid, delay, runtime, max_requests)
        res = run_dnsperf(dnsperfconfig)
        q, a, v = parse_dnsperf_output(res)
        print(f"QPS: {mid:,}, rQPS:{rqps:,} Queries: {q:,}, Answers: {a:,}, Valid: {v:,}\n")
        if q != v:
            high = mid
        else:
            # save log file
            shutil.move("dns64perf.csv", log_target)
            low = mid
       
    return low

def get_args():
    parser = argparse.ArgumentParser(
                    prog='DNS64perf++ binary search',
                    description='Search for the maximum QPS for a given zone file',
                    epilog='2025 Andreas Levander')
    
    parser.add_argument('-f', '--file', help='config file name', required=True)

    return parser.parse_args().file

def main():
    before = datetime.datetime.now()
    cnfg_file = get_args()
    config = configparser.ConfigParser()
    files = config.read(cnfg_file)
    if not files:
        raise Exception(f"Could not read config file {cnfg_file}")

    dnsperfconfig = config['dns64perfpp']

    targetname = config['DEFAULT']['targetname']
    path = f"./logs/{targetname}"
    if not os.path.isdir(path):
        os.makedirs(path)

    runtime = int(config['dns64perfpp']['runtime'])
    accuracy = int(config['DEFAULT']['accuracy'])
    start_qps = int(config['DEFAULT']['start_qps'])
    max_qps = int(config['DEFAULT']['max_qps'])
    runs = int(config['DEFAULT']['runs'])
    delay = int(config['dns64perfpp']['burst_delay'])
    MAX_REQUESTS = calculate_max_requests(dnsperfconfig)


    for run in range(1, runs + 1):
        rt = datetime.datetime.now()
        print("\n********************************")
        print(f"Starting run {run}")
        log_target = f"{path}/{targetname}_{run}.csv"
        qps = binary_searchQPS(dnsperfconfig, start_qps, max_qps, runtime, accuracy, MAX_REQUESTS, log_target, delay)
        print(f"Max QPS: {qps * int(config['dns64perfpp']['threads']):,} for run {run}")
        
        print(f"Run {run} done in {datetime.datetime.now() - rt}")
    
    after = datetime.datetime.now()
    print(f"Total time taken: {after - before}")

if __name__ == "__main__":
    main()