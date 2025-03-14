# -*- coding: utf-8 -*-
import time
import socket
import os
from baselines.oss import oss
    

def get_local_ip():
    """获取本机局域网 IP 地址，避免返回 127.0.0.1"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # 连接到一个公共地址，不需要实际发送数据
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()

def get_worker_key():
    return f"{get_local_ip()}_{os.getpid()}"
        

DEFAULT_LOCK_FILE = "oss://si002558te8h/dclm/process_lockfile"
        
class SimpleOSSLock:
    def __init__(self, lock_file: str) -> None:
        self.bucket_name, self.path = oss.split_file_path(lock_file)
        self.bucket = oss.Bucket(self.bucket_name)
        self.local_ip = get_local_ip()
        self.process_id = os.getpid()
        self.lock_value = f"locked_{self.local_ip}_{self.process_id}"
        
    def acquire(self) -> bool:
        try:
            self.bucket.put_object(self.path, self.lock_value, headers={"x-oss-forbid-overwrite": "true"})
            return True
        except BaseException:
            return False

    def acquire_or_block(self, timeout=100) -> bool:
        if timeout == -1:
            while True:
                if self.acquire():
                    return True
                time.sleep(1)
        else:
            count = int(timeout / 1)
            while count > 0:
                if self.acquire():
                    return True
                time.sleep(1)
                count -= 1
        return False

    def release(self) -> bool:
        try:
            lock_content = self.bucket.get_object(self.path).read().decode('utf-8')
            if lock_content == self.lock_value:
                self.bucket.delete_object(self.path)
                return True
            else:
                return False
        except BaseException:
            return False        
                
