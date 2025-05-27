mod worker;
mod conn_db;
mod least_conn_server;

use libc::*;
use num_cpus;
use std::net::Ipv4Addr;
use worker::worker_loop;
use conn_db::manage_connections;

fn set_nonblocking(fd: libc::c_int) {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL, 0);
        if flags < 0 {
            panic!("fcntl F_GETFL failed");
        }

        if libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
            panic!("fcntl F_SETFL O_NONBLOCK failed");
        }
    }
}

fn main() {
    unsafe {
        let sock_fd = socket(AF_INET, SOCK_STREAM, 0);
        let yes = 1;
        setsockopt(
            sock_fd,
            SOL_SOCKET,
            SO_REUSEADDR,
            &yes as *const _ as *const _,
            size_of::<i32>() as u32,
        );
        setsockopt(
            sock_fd,
            SOL_SOCKET,
            SO_REUSEPORT,
            &yes as *const _ as *const _,
            size_of::<i32>() as u32,
        );
        let ip: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1).into();
        let addr = sockaddr_in {
            sin_family: AF_INET as u8,
            sin_port: htons(8080),
            sin_addr: in_addr {
                s_addr: u32::from_ne_bytes(ip.octets()),
            },
            sin_zero: [0; 8],
            sin_len: size_of::<sockaddr_in>() as u8,
        };

        if bind(
            sock_fd,
            &addr as *const _ as *const sockaddr,
            size_of::<sockaddr_in>() as u32,
        ) < 0
        {
            panic!("Bind failed...");
        }

        set_nonblocking(sock_fd);

        if listen(sock_fd, 10) < 0 {
            panic!("listen failed");
        }
        println!("Listening on 127.0.0.1:8080");

        let cpu_count: usize = num_cpus::get();
        let mut workers: Vec<i32> = Vec::new();

        let conn_db_pid = fork();
        if conn_db_pid == 0 {
            manage_connections(cpu_count-1);
            std::process::exit(0);
        } else if conn_db_pid > 0 {
            
        } else {
            panic!("Fork Failed...");
        }

        for _ in 0..(cpu_count - 2) {
            let pid = fork();
            if pid == 0 {
                worker_loop(sock_fd);
                std::process::exit(0);
            } else if pid > 0 {
                workers.push(pid);
            } else {
                panic!("Fork Failed...");
            }
        }


        loop {
            libc::pause();
        }
    }
}
