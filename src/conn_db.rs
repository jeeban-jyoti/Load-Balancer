use libc::*;
use std::ffi::CString;
use std::mem::{self, zeroed};
use std::ptr;
use std::sync::{Arc, Mutex};

use crate::least_conn_server::LCS;

const SOCK_PATH: &str = "/tmp/test1.sock";

fn ev_set(
    kev: &mut libc::kevent,
    ident: libc::uintptr_t,
    filter: libc::c_int,
    flags: libc::c_ushort,
    fflags: libc::c_uint,
    data: libc::intptr_t,
    udata: *mut libc::c_void,
) {
    kev.ident = ident;
    kev.filter = filter as i16;
    kev.flags = flags;
    kev.fflags = fflags;
    kev.data = data;
    kev.udata = udata;
}

pub fn manage_connections(worker_count: usize) {
    unsafe {
        let sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if sock_fd < 0 {
            panic!("Socket creation failed");
        }

        let _ = std::fs::remove_file(SOCK_PATH);
        let mut addr: sockaddr_un = mem::zeroed();
        addr.sun_family = AF_UNIX as sa_family_t;
        let path = CString::new(SOCK_PATH).unwrap();
        ptr::copy_nonoverlapping(
            path.as_ptr(),
            addr.sun_path.as_mut_ptr() as *mut i8,
            path.as_bytes().len(),
        );
        let addr_len = (std::mem::size_of::<sa_family_t>() + path.as_bytes().len()) as u32;

        if bind(sock_fd, &addr as *const _ as *const sockaddr, addr_len) < 0 {
            panic!("bind failed");
        }

        if listen(sock_fd, worker_count as i32) < 0 {
            panic!("listen failed");
        }

        println!("Server listening on {}", SOCK_PATH);

        let server_lcs = Arc::new(Mutex::new(LCS::new()));
        let server_lcs_data = Arc::clone(&server_lcs);
        {
            let mut data = server_lcs_data.lock().unwrap();
            let servers: Vec<[u8; 6]> = vec![
                [127, 0, 0, 1, 11, 184],
                [127, 0, 0, 1, 11, 185],
                [127, 0, 0, 1, 11, 186],
                [127, 0, 0, 1, 11, 187],
                [127, 0, 0, 1, 11, 188],
                [127, 0, 0, 1, 11, 189],
                [127, 0, 0, 1, 11, 190],
                [127, 0, 0, 1, 11, 191],
                [127, 0, 0, 1, 11, 192],
                [127, 0, 0, 1, 11, 193],
                [127, 0, 0, 1, 11, 194],
            ];
            for server in &servers {
                data.insert(&server).expect("Insert failed");
            }
        }

        let kq = kqueue();
        let mut ev: kevent = zeroed();
        ev_set(
            &mut ev,
            sock_fd as usize,
            EVFILT_READ as i32,
            EV_ADD | EV_ENABLE,
            0,
            0,
            ptr::null_mut(),
        );

        let _ = kevent(kq, &ev, 1, ptr::null_mut(), 0, ptr::null());

        loop {
            let mut events: [kevent; 32] = zeroed();
            let nev = kevent(kq, ptr::null(), 0, events.as_mut_ptr(), 32, ptr::null());

            if nev < 0 {
                eprintln!("kevent error");
                break;
            }

            let mut data = server_lcs_data.lock().unwrap();
            for i in 0..nev {
                let ev = events[i as usize];

                if ev.filter == EVFILT_READ {
                    if ev.ident == sock_fd as usize {
                        let client_fd = accept(sock_fd, ptr::null_mut(), ptr::null_mut());
                        if client_fd >= 0 {
                            println!(
                                "Accepted connection fd: {} by {}",
                                client_fd,
                                std::process::id()
                            );
                            let mut client_ev: kevent = zeroed();
                            ev_set(
                                &mut client_ev,
                                client_fd as usize,
                                EVFILT_READ as i32,
                                EV_ADD | EV_ENABLE,
                                0,
                                0,
                                ptr::null_mut(),
                            );

                            let _ = kevent(kq, &client_ev, 1, ptr::null_mut(), 0, ptr::null());
                        }
                    } else {
                        let client_fd = ev.ident as i32;

                        if client_fd >= 0 {
                            // println!(
                            //     "Accepted connection fd at conn_db: {} by {}",
                            //     client_fd,
                            //     std::process::id()
                            // );
                            let mut buf = [0u8; 21];
                            let n = read(client_fd, buf.as_mut_ptr() as *mut _, 4096);
                            if n == 0 {
                                let mut ev: kevent = mem::zeroed();
                                ev_set(
                                    &mut ev,
                                    client_fd as uintptr_t,
                                    libc::EVFILT_READ as i32,
                                    libc::EV_DELETE,
                                    0,
                                    0,
                                    std::ptr::null_mut(),
                                );

                                let res =
                                    kevent(kq, &ev, 1, std::ptr::null_mut(), 0, std::ptr::null());

                                if res < 0 {
                                    eprintln!("Failed to delete fd {} from kqueue", client_fd);
                                }
                            } else if n > 0 {
                                let req_type = buf[0];
                                let server: [u8; 6] = buf[1..7]
                                    .try_into()
                                    .expect("something went wrong in slicing");
                                match req_type {
                                    0 => {
                                        let mut response = [0u8; 10];
                                        let server = data.get_least_conn_server().unwrap();
                                        response[..6].copy_from_slice(&server);
                                        response[6..].copy_from_slice(&buf[3..7]);

                                        // let stats = data.get_stats().unwrap();
                                        // for (key, val) in stats {
                                        //     println!("{:?} : {}", key, val);
                                        // }
                                        write(client_fd, response.as_ptr() as *const _, response.len());
                                        let _ = data.server_conn_increament(&server);
                                    }
                                    1 => {
                                        let _ = data.server_conn_decreament(&server);
                                        // let stats = data.get_stats().unwrap();
                                        // for (key, val) in stats {
                                        //     println!("{:?} : {}", key, val);
                                        // }
                                    }
                                    2 => {
                                        let _ = data.delete(&server);
                                        // let stats = data.get_stats().unwrap();
                                        // for (key, val) in stats {
                                        //     println!("{:?} : {}", key, val);
                                        // }
                                    }
                                    _ => {}
                                };
                            } else {
                                let mut ev: kevent = mem::zeroed();
                                ev_set(
                                    &mut ev,
                                    client_fd as uintptr_t,
                                    libc::EVFILT_READ as i32,
                                    libc::EV_DELETE,
                                    0,
                                    0,
                                    std::ptr::null_mut(),
                                );

                                let res =
                                    kevent(kq, &ev, 1, std::ptr::null_mut(), 0, std::ptr::null());

                                if res < 0 {
                                    eprintln!("Failed to delete fd {} from kqueue", client_fd);
                                }
                            }
                        }
                    }
                }
            }
        }
        close(sock_fd);
        close(kq);
    }
}
