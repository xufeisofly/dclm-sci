mod oss;
use std::env;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // 计数器文件路径（多个任务共享同一个文件）
    let file_path = "counter.txt";
    let file_path = Arc::new(file_path.to_string());

    // 如果文件不存在，则先创建并写入 "0"
    if !Path::new(&*file_path).exists() {
        let mut file = File::create(&*file_path).expect("创建文件失败");
        file.write_all(b"0").expect("写入初始值失败");
    }

    let args: Vec<String> = env::args().collect();
    let task_count: usize = if args.len() > 1 {
        args[1].parse().unwrap_or(10)
    } else {
        10
    };
    println!("启动 {} 个任务", task_count);

    // 启动多个任务，模拟多个进程/线程并发
    let mut handles = Vec::new();

    for _ in 0..task_count {
        let file_path = file_path.clone();
        let handle = tokio::spawn(async move {
            let lock_file = "oss://si002558te8h/dclm/dedupe_lockfile";
            let lock = oss::SimpleOSSLock::new(lock_file).expect("创建锁失败");

            // 尝试在 500 秒内获取锁（这里设置了较长的等待时间，实际测试时可调整）
            if lock.acquire_or_block(500).await {
                println!("任务 {:?}: 锁已获取！", std::thread::current().id());
                // 使用 spawn_blocking 包装阻塞的文件操作
                let result = tokio::task::spawn_blocking({
                    let file_path = file_path.clone();
                    move || -> std::io::Result<()> {
                        // 读取当前计数器值
                        let mut contents = String::new();
                        {
                            let mut file = OpenOptions::new().read(true).open(&*file_path)?;
                            file.read_to_string(&mut contents)?;
                        }
                        let count: i32 = contents.trim().parse().unwrap_or(0);

                        // 将计数器加 1 后写回文件（覆盖写入）
                        {
                            let mut file = OpenOptions::new()
                                .write(true)
                                .truncate(true)
                                .open(&*file_path)?;
                            file.write_all(format!("{}", count + 1).as_bytes())?;
                        }
                        Ok(())
                    }
                })
                .await;

                match result {
                    Ok(Ok(())) => {
                        println!("任务 {:?}: 计数器更新成功！", std::thread::current().id())
                    }
                    Ok(Err(e)) => eprintln!(
                        "任务 {:?}: 文件操作错误: {}",
                        std::thread::current().id(),
                        e
                    ),
                    Err(e) => eprintln!(
                        "任务 {:?}: spawn_blocking 异常: {:?}",
                        std::thread::current().id(),
                        e
                    ),
                }

                if lock.release().await {
                    println!("任务 {:?}: 锁已释放！", std::thread::current().id());
                } else {
                    println!("任务 {:?}: 释放锁失败", std::thread::current().id());
                }
            } else {
                println!(
                    "任务 {:?}: 在超时时间内未能获取锁",
                    std::thread::current().id()
                );
            }
        });
        handles.push(handle);
    }

    // 等待所有任务完成
    for handle in handles {
        let _ = handle.await;
    }

    // 最后读取文件中计数器的值
    let mut final_contents = String::new();
    {
        let mut file = File::open(&*file_path).expect("打开计数器文件失败");
        file.read_to_string(&mut final_contents)
            .expect("读取计数器文件失败");
    }
    println!("最终计数器值：{}", final_contents.trim());
}
