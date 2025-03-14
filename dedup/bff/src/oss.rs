use oss_rust_sdk::oss::OSS;
use std::env;
use std::net::UdpSocket;
use std::process;
use std::thread;
use std::time::Duration;

/// 获取指定 bucket 的 OSS 实例，若已存在则直接复用
pub fn get_bucket(bucket_name: String) -> OSS<'static> {
    // 否则，新建并插入到缓存中
    let access_id = env::var("OSS_ACCESS_KEY_ID").unwrap();
    let access_secret = env::var("OSS_ACCESS_KEY_SECRET").unwrap();
    // println!("access_id: {access_id}, access_secret: {access_secret}");

    let endpoint = "http://oss-cn-hangzhou-zjy-d01-a.ops.cloud.zhejianglab.com/";
    let new_oss = OSS::new(access_id, access_secret, endpoint.into(), bucket_name);
    new_oss
}

/// 从 oss://bucket_name/path 格式的 URL 中解析出 bucket_name 和 path
fn split_file_path(url: &str) -> Result<(String, String), String> {
    if !url.starts_with("oss://") {
        return Err("Invalid OSS URL".into());
    }
    let rest = &url[6..]; // 去除 "oss://"
    let parts: Vec<&str> = rest.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err("Invalid OSS URL, missing path".into());
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// 获取本机局域网 IP 地址（不返回 127.0.0.1）
fn get_local_ip() -> String {
    if let Ok(socket) = UdpSocket::bind("0.0.0.0:0") {
        if socket.connect("8.8.8.8:80").is_ok() {
            if let Ok(local_addr) = socket.local_addr() {
                return local_addr.ip().to_string();
            }
        }
    }
    "127.0.0.1".to_string()
}

/// 根据本机 IP 和进程号生成唯一标识
fn get_worker_key() -> String {
    format!("{}_{}", get_local_ip(), process::id())
}

/// 分布式锁
pub struct SimpleOSSLock {
    bucket: OSS<'static>,
    path: String,
    local_ip: String,
    process_id: u32,
    lock_value: String,
}

impl SimpleOSSLock {
    /// 创建一个新的 SimpleOSSLock 实例
    pub fn new(lock_file: &str) -> Result<Self, String> {
        let (bucket_name, path) = split_file_path(lock_file)?;
        let bucket = get_bucket(bucket_name);
        let local_ip = get_local_ip();
        let process_id = process::id();
        let lock_value = format!("locked_{}_{}", local_ip, process_id);
        Ok(SimpleOSSLock {
            bucket,
            path,
            local_ip,
            process_id,
            lock_value,
        })
    }

    /// 尝试获取锁，成功返回 true，否则返回 false
    pub fn acquire(&self) -> bool {
        let mut headers = HashMap::new();
        headers.insert("x-oss-forbid-overwrite".to_string(), "true".to_string());
        self.bucket
            .put_object(&self.path, &self.lock_value, headers, None)
            .await;
    }

    /// 在规定超时时间内不断尝试获取锁。timeout 为 -1 表示无限等待（每次间隔 1 秒）
    pub fn acquire_or_block(&self, timeout: i32) -> bool {
        if timeout == -1 {
            loop {
                if self.acquire() {
                    return true;
                }
                thread::sleep(Duration::from_secs(1));
            }
        } else {
            let mut count = timeout;
            while count > 0 {
                if self.acquire() {
                    return true;
                }
                thread::sleep(Duration::from_secs(1));
                count -= 1;
            }
            false
        }
    }

    /// 释放锁：先获取锁文件内容，若与当前进程的 lock_value 匹配则删除锁文件
    pub fn release(&self) -> bool {
        match self
            .bucket
            .get_object(&self.path, None::<HashMap<&str, &str>>, None)
            .await
        {
            Ok(content) => {
                if content == self.lock_value {
                    self.bucket.delete_object(&self.path).await;
                } else {
                    false
                }
            }
            Err(_) => false,
        }
    }
}
