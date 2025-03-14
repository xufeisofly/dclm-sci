# -*- coding: utf-8 -*-
import os
import argparse
import json
from baselines.oss import oss
from typing import List

"""
可以用这个脚本生成 task 用于分片，也可以自己写 tasks.json 文件
asign_task.py parses the dclm input directory and split tasks, generating a tasks.json file
for all the ray_process.py to accept.

{
  tasks: [
    {
      "shard_dir": "oss://si002558te8h/dclm/origin/CC-MAIN-2014-51/",
      "file_range": [0, -1],
      "worker": {
        "key": "192.168.0.2_828282",
        "status": "processing" // processing/success/fail
      } // or null
    }, {
      // ...
    }
  ]
}
"""

class TaskItem:
    def __init__(self, shard_dir, file_range: List[int], worker=None) -> None:
        self._shard_dir = shard_dir
        self._file_range = file_range
        self._worker = worker

    def get_shard_dir(self):
        return self._shard_dir

    def to_dict(self) -> dict:
        return {
            "shard_dir": self._shard_dir,
            "file_range": self._file_range,
            "worker": self._worker,
        }

def create_task_items(shard_dirs: List[str], mode: str) -> List[dict]:
    tasks = []
    for shard_dir in shard_dirs:
        if mode == 'dedup':
            # 对于 dedup 任务，CC-MAIN 目录下还有一层 processed_data
            shard_dir = [os.path.join(shard_dir, 'processed_data')]
        tasks.append(TaskItem(shard_dir, [0, -1]).to_dict())
    return tasks

    
def asign_task(parent_dir: str, tasks_file_path: str, mode: str='process'):
    bucket_name, path = oss.split_file_path(parent_dir) 
    bucket = oss.Bucket(bucket_name)
    rets = bucket.list_objects_v2(prefix=path, delimiter='/').prefix_list
    shard_dirs = [os.path.join("oss://" + bucket_name, ret) for ret in rets if ret.endswith('/') and 'CC-MAIN' in ret]

    task_items = create_task_items(shard_dirs, mode)
    data = {
        "tasks": task_items,
    }
    
    with oss.OSSPath(tasks_file_path).open("w") as f:
        f.write(json.dumps(data, indent=4))
    
    task_bucket_name, task_file = oss.split_file_path(tasks_file_path)
    existed = oss.Bucket(task_bucket_name).object_exists(task_file)

    if existed:
        print(f"Success: {len(task_items)} tasks generated")
    else:
        print(f"Failed")

        
DEFAULT_TASKS_FILE_PATH = "oss://si002558te8h/dclm/process_tasks.jsonl"
DEFAULT_PARENT_DIR = "oss://si002558te8h/dclm/origin/"


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--parent_dir", help="", type=str, default=DEFAULT_PARENT_DIR)
    parser.add_argument("--tasks_file_path", help="", type=str, default=DEFAULT_TASKS_FILE_PATH)
    parser.add_argument("--mode", help="process/dedup", type=str, default='process')
    args = parser.parse_args()    
    asign_task(args.parent_dir, args.tasks_file_path, args.mode)
