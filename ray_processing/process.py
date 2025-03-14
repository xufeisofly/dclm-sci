import boto3
import time
import os
import argparse
from tqdm import tqdm
from yaml import safe_load
import glob
import subprocess
import json

from baselines.core import process_single_file
from baselines.core.file_utils import read_jsonl, write_jsonl, delete_file, is_exists, is_s3, is_oss
from baselines.oss import oss
from baselines.oss.lock import SimpleOSSLock, DEFAULT_LOCK_FILE, get_worker_key
from ray_processing import GLOBAL_FUNCTIONS
from ray_processing.utils import generate_untokenized_dataset_json, get_source_ref, get_source_ref_by_key
from task_asigning.asign_task import DEFAULT_TASKS_FILE_PATH, TaskItem

import ray
import traceback
import warnings


RAY_CHUNK_SUCCESS = 1
RAY_CHUNK_FAILURE = 0
LOCAL_CHUNK = "local"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source_ref_paths", help="paths to untokenized datasets refs, comma or space separated", type=str, nargs="+"
    )
    parser.add_argument(
        "--raw_data_dirpath",
        help="the path to the top data directory in the data hierarchy (from which to mirror the output path)",
    )
    parser.add_argument("--shard_list_file", type=str, default=None, help="Path to a file containing a list of input shards.")
    parser.add_argument(
        "--shard_list_filters", type=str, nargs='+', help="List of substrings to filter the input shard list by."
    )

    parser.add_argument("--output_dir", required=True, help="Path to the output dir of the processed file.")
    parser.add_argument(
        "--readable_name", required=True, type=str, help="name given to tokenized dataset and reference json file name"
    )

    parser.add_argument(
        "--config_path",
        default="baselines/baselines_configs/c4.yaml",
        help="Path to the YAML file specifying the baseline.",
    )
    parser.add_argument(
        "--source_name", type=str, default="dcnlp_beta_pool", help="The name of the source of the jsonl file."
    )
    parser.add_argument(
        "--workers", type=int, default=1, help="If > 1, will use a process pool with that many workers."
    )
    parser.add_argument("--overwrite", action="store_true", help="If set to true, will overwrite results.")
    parser.add_argument("--ray_address", type=str, default="localhost:6379")
    parser.add_argument("--num_shards", type=int, default=None, help="Run on the first number of shards (for debugging)")
    parser.add_argument(
        "--ignore_failures", action="store_true", help="Skip steps if there are partial failures. Use sparingly."
    )
    parser.add_argument("--ray_use_working_dir", action='store_true', help="Working directory for ray.")
    parser.add_argument("--ray_num_cpus", type=int, default=1, help="Number of CPUs to use for each ray task.")

    return parser.parse_args()


# Right now, this is just how I get clear space in /tmp which quickly gets filled by s3 reads
@ray.remote(max_calls=3)
def process_local_chunk(
    config_data, raw_data_dirpath, jsonl_relpath, source_name, base_output_path, workers, overwrite
):
    try:
        _, _, pages_in, pages_out = process_single_file(
            config_data=config_data,
            raw_data_dirpath=raw_data_dirpath,
            jsonl_relpath=jsonl_relpath,
            source_name=source_name,
            base_output_path=base_output_path,
            workers=workers,
            overwrite=overwrite,
        )
        return RAY_CHUNK_SUCCESS, pages_in, pages_out
    except Exception as e:
        traceback.print_exc()
        return RAY_CHUNK_FAILURE, 0, 0


def to_iterator(obj_ids, batch_size=100):
    while obj_ids:
        done, obj_ids = ray.wait(obj_ids, num_returns=min(batch_size, len(obj_ids)))
        for d in done:
            yield ray.get(d)


def list_shard_files(data_dirpath, num_shards=None, shard_list_file=None, shard_list_filters=None):
    assert bool(shard_list_file) ^ bool(data_dirpath), "Either shard_list_file or data_dirpath must be provided, but not both."
    
    def get_files_in_directory(data_dirpath):
        # 获取指定目录下的所有文件
        files = [f for f in os.listdir(data_dirpath) if all(s not in f for s in ['stats', 'global_stats.json'])]
        return files


    if shard_list_file is not None:
        with open(shard_list_file, "r") as f:
            shard_files = f.read().splitlines()
    elif is_s3(data_dirpath):
        s3 = boto3.resource('s3')
        bucket_name, path_within_bucket = data_dirpath.replace("s3://","").split("/", 1)
        path_within_bucket = path_within_bucket if path_within_bucket.endswith("/") else f'{path_within_bucket}/'
        bucket = s3.Bucket(bucket_name)
        shard_files = [x.key.replace(path_within_bucket, "") 
                       for x in bucket.objects.filter(Prefix=path_within_bucket) 
                       if all(s not in x.key for s in ['/stats/', 'global_stats.jsonl'])]
    elif is_oss(data_dirpath):
        bucket_name, path_within_bucket = data_dirpath.replace("oss://", "").split("/", 1)
        path_within_bucket = path_within_bucket if path_within_bucket.endswith("/") else f'{path_within_bucket}/'
        bucket = oss.Bucket(bucket_name)
        shard_files = [x.key.replace(path_within_bucket, "")
                       for x in bucket.list_objects(prefix=path_within_bucket).object_list
                       if all(s not in x.key for s in ['/stats/', 'global_stats.jsonl']) and not x.key.endswith('/')]
    else:
        shard_files = get_files_in_directory(data_dirpath=data_dirpath)

    if num_shards is not None:
        shard_files = shard_files[:num_shards]

    if shard_list_filters is not None:
        shard_files = [s for s in shard_files if any(f in s for f in shard_list_filters)]

    return shard_files


def get_task_item():
    asigned_task = None
    lock = SimpleOSSLock(DEFAULT_LOCK_FILE)
    # 分布式锁允许 1 hour 超时时间
    if lock.acquire_or_block(timeout=3600):
        # 改写 tasks.json 文件，领取任务
        ret = ""
        try:
            with oss.OSSPath(DEFAULT_TASKS_FILE_PATH).open("rb") as f:
                data = f.read()
                ret = json.loads(data)
            task_items = ret['tasks']

            for i, task_item in enumerate(task_items):
                if task_item['worker'] is not None:
                    continue
                asigned_task = task_item
                task_items[i]['worker'] = {
                    'key': get_worker_key(),
                    'status': 'processing'
                }
                break

            if asigned_task is None:
                lock.release()
                return None

            new_data = {
                'tasks': task_items,
            }
            with oss.OSSPath(DEFAULT_TASKS_FILE_PATH).open("w") as f:
                f.write(json.dumps(new_data, indent=4))

            lock.release()
            return TaskItem(asigned_task['shard_dir'], asigned_task['file_range'])
        except BaseException as e:
            print(f"get task item failed: {e}")
            lock.release()
            return None
    else:
        print(f"Worker {get_worker_key()} could not acquire the lock within timeout.")
        return None

def process_all(mode='task'):
    with_init = True 
    while mode == 'task':
        task_item = get_task_item()
        if task_item is None:
            return
        process_task_item(task_item, with_init)
        with_init = False
    process_task_item(None, with_init)

def process_task_item(task_item: TaskItem|None, with_init=True):
    os.environ["RAY_LOG_TO_STDERR"] = "1"
    args = parse_args()

    task_input_dirpath, shard_name = '', ''
    # json_path 文件用于检测该 input 是否曾经跑完
    # TODO 由于 worker 任务是随机认领的，这个 json 文件最好放在 oss 上
    json_path = f"exp_data/datasets/untokenized/{args.readable_name}.json"
    if task_item is not None:
        shard_dir = task_item.get_shard_dir()
        shard_name = shard_dir.split('/')[-2]
        task_input_dirpath = shard_dir
        json_path = f"exp_data/datasets/untokenized/{args.readable_name}_{shard_name}.json"
        print(f"task shard dir: {shard_dir}")    
    if not args.overwrite:
        assert not os.path.exists(
            json_path
        ), f"{json_path} already exists. Try changing --readable_name or deleting the existing file."

    source_refs = None
    if args.source_ref_paths is not None:
        source_ref_paths = [p.strip() for paths in args.source_ref_paths for p in paths.split(",") if p.strip()]
        source_refs = [get_source_ref(s) for s in source_ref_paths]
        assert len(source_refs) == 1, "For now only one source is supported"
        args.raw_data_dirpath = source_refs[0]["dataset_url"]
    else:
        source_ref = get_source_ref_by_key(args.raw_data_dirpath, "dataset_url")
        source_refs = [source_ref] if source_ref else []

    if with_init:
        if args.ray_use_working_dir:
            ray.init(address=args.ray_address, runtime_env={"working_dir": "./", "excludes": ["tests/"]})
        else:
            ray.init(address=args.ray_address)

    config_path = args.config_path
    
    def get_output_dir(output, shard_name):
        if shard_name == '':
            return os.path.join(output, args.readable_name)
        # output dir: oss://si002558te8h/dclm/output/sci_test/CC-MAIN-2014-11/
        return os.path.join(output, args.readable_name, shard_name)    
    
    output_dir = get_output_dir(args.output_dir, shard_name)
    source_name = args.source_name
    
    # base output path 去掉 config_name，使用自定义的 readable name 去区分不同的 pipeline
    # config_name = os.path.basename(config_path).split(".")[0]
    # base_output_path = os.path.join(output_dir, config_name)
    base_output_path = output_dir

    # Collect the global stats file, which is used to record / resume a data processing pipeline
    global_stats_path = os.path.join(base_output_path, "global_stats.jsonl")
    global_stats = []
    if is_exists(global_stats_path):
        if args.overwrite:
            delete_file(global_stats_path)
        else:
            try:
                global_stats = list(read_jsonl(global_stats_path))
            except BaseException:
                global_stats = []

    # Process the yaml file into chunks of either contiguous local functions OR single global functions
    with open(config_path, "r") as yaml_file:
        config_data = safe_load(yaml_file)
        config_data = {v["source"]: v for v in config_data}
    source_data = config_data[source_name]
    steps = source_data["steps"]

    chunks = []  # Contains either the global function specification or LOCAL_CHUNK for a local chunk
    prev_step_global = True  # Keeps track of whether the last step seen was global
    for s in steps:
        if "func" in s and s["func"] in GLOBAL_FUNCTIONS:
            if len(chunks) == 0:
                raise Exception("Using a global operation as the first step is not currently supported.")
            chunks.append(s)
            prev_step_global = True
        else:
            if prev_step_global:
                chunks.append(LOCAL_CHUNK)
            prev_step_global = False

    # Begin processing the chunks
    true_start = time.time()
    working_dir = task_input_dirpath if task_input_dirpath != '' else args.raw_data_dirpath
    overwrite = args.overwrite

    for i, c in enumerate(chunks):
        chunk_start = time.time()
        step_name = LOCAL_CHUNK if c == LOCAL_CHUNK else c["func"]
        resumed_chunk = False
        # xufeisofly
        shard_files = list_shard_files(working_dir, args.num_shards, args.shard_list_file)
        shard_extension = os.path.splitext(shard_files[0])[-1][1:]

        # If chunk has already been processed according to global stats, then skip it
        if i < len(global_stats) and step_name == global_stats[i]["name"]:
            # TODO: Right now, only local chunks will output a num_failures
            num_failures = global_stats[i].get("num_failures", 0)
            if num_failures == 0 or args.ignore_failures:
                if num_failures > 0:
                    warnings.warn(
                        f"{num_failure} failures are being ignored, which may significantly and unpredictably impact final results."
                    )
                print(f"Skipping chunk {i} with name {step_name}")
                working_dir = global_stats[i]["working_dir"]
                continue
            elif num_failures > 0 and not args.overwrite:
                resumed_chunk = True
                working_dir = global_stats[i - 1]["working_dir"] if i > 0 else working_dir

        # Retrieve the list of files before processing a chunk (in case of deletions)

        print(f"Starting chunk {i} with name {step_name}, # of input jsonls = {len(shard_files)}")

        if resumed_chunk:
            shard_files = global_stats[i]["failed_shards"]

        # Process the chunk according to whether it is local or global
        if c == LOCAL_CHUNK:
            ret = []
            for idx, jsonl_relpath in enumerate(shard_files):
                ret.append(
                    process_local_chunk.options(num_cpus=args.ray_num_cpus).remote(
                        config_data, working_dir, jsonl_relpath, source_name, base_output_path, args.workers, overwrite
                    )
                )
            for x in tqdm(to_iterator(ret), total=len(ret)):
                pass

            ret = ray.get(ret)
            successes = sum(r[0] for r in ret)
            failures = len(ret) - successes
            pages_in = sum(r[1] for r in ret)
            pages_out = sum(r[2] for r in ret)
            failed_shards = [s for i, s in enumerate(shard_files) if ret[i][0] == RAY_CHUNK_FAILURE]

            # Make sure the working_dir has processed_data/ at the end
            working_dir = os.path.join(base_output_path, "processed_data/")

            # If resuming a chunk that partially errored, update the global stats instead of appending a new row
            if resumed_chunk:
                # Erase the record of the subsequent steps, since they will now be affected
                global_stats = global_stats[: i + 1]
                global_stats[i]["resumptions"] += 1
                global_stats[i]["secs"] += time.time() - chunk_start
                global_stats[i]["pages_in"] += sum(r[1] for i, r in enumerate(ret))
                global_stats[i]["pages_out"] += sum(r[2] for i, r in enumerate(ret))
                global_stats[i].update(
                    {"num_successes": successes, "num_failures": failures, "failed_shards": failed_shards}
                )
            else:
                global_stats.append(
                    {
                        "name": LOCAL_CHUNK,
                        "secs": time.time() - chunk_start,
                        "num_successes": successes,
                        "num_failures": failures,
                        "pages_in": pages_in,
                        "pages_out": pages_out,
                        "working_dir": working_dir,
                        "resumptions": 0,
                        "failed_shards": failed_shards,
                    }
                )

            overwrite = False
            write_jsonl(global_stats, global_stats_path, "w")

            if failures > 0:
                warnings.warn(
                    f"Local chunk failed on {failures} shards out of {len(ret)}. This may significantly and unpredictably affect final results. Re-running this local chunk by using the same yaml config and turning off the --ignore_failures flag."
                )
                if not args.ignore_failures:

                    raise Exception(f"Exiting due to local failures. ")
        else:
            step = c
            kwargs = {k: v for k, v in step.items() if k not in ["func"]}

            # Assumption: Global functions will return a working directory
            working_dir = GLOBAL_FUNCTIONS[step["func"]](working_dir, shard_files, base_output_path, **kwargs)
            global_stats.append({"name": step["func"], "secs": time.time() - chunk_start, "working_dir": working_dir})

            # If the last step and working_dir is not already the desired base_output_path, make sure to sync
            if i == len(chunks) - 1 and base_output_path != working_dir:
                print(f"Final sync required back to desired ouput path: from {working_dir} to {base_output_path}")
                sync_list = ["aws", "s3", "sync", working_dir, base_output_path]
                process = subprocess.Popen(sync_list)
                process.wait()
            write_jsonl(global_stats, global_stats_path, "w")

        print("Chunk time: " + str(time.time() - chunk_start))
    print("Total time: " + str(time.time() - true_start))

    # Generate the dataset reference json
    dataset_json = generate_untokenized_dataset_json(args, source_refs, base_output_path, data_key=shard_extension)
    with open(json_path, "w") as ref_file:
        json.dump(dataset_json, ref_file, indent=4)


if __name__ == "__main__":
    process_all(mode='task')
