use http::{request, Version};
use libc::*;
use std::collections::{HashMap, HashSet};
use std::ffi::{CStr,CString};
// use std::io::Read;
use std::mem::{self, zeroed};
use std::net::Ipv4Addr;
use std::os::fd::RawFd;
use std::ptr;
use http::{Request, header::{HeaderName, HeaderValue}};
use httparse::{Request as HttpParseRequest, Status};

extern crate queues;
// use queues::*;

const SOCK_PATH: &str = "/tmp/test1.sock";

fn ev_set(
    kev: &mut libc::kevent,
    ident: libc::uintptr_t,
    filter: libc::c_int,
    flags: libc::c_ushort,
    fflags: libc::c_uint,
    data: libc::intptr_t,
    udata: *mut libc::c_void
) {
    kev.ident = ident;
    kev.filter = filter as i16;
    kev.flags = flags;
    kev.fflags = fflags;
    kev.data = data;
    kev.udata = udata;
}

fn add_fd_to_kqueue(kq: i32, fd: usize) {
    unsafe {
        let mut ev: kevent = zeroed();
        ev_set(
            &mut ev,
            fd,
            EVFILT_READ as i32,
            EV_ADD | EV_ENABLE,
            0,
            0,
            ptr::null_mut(),
        );

        let _ = kevent(kq, &ev, 1, ptr::null_mut(), 0, ptr::null());
    }
}

fn del_fd_to_kqueue(kq: i32, fd: usize) {
    unsafe {
        let mut ev: kevent = mem::zeroed();
        ev_set(
            &mut ev,
            fd,
            libc::EVFILT_READ as i32,
            libc::EV_DELETE,
            0,
            0,
            std::ptr::null_mut(),
        );

        let _ = kevent(kq, &ev, 1, std::ptr::null_mut(), 0, std::ptr::null());
    }
}

const INET_ADDRSTRLEN: usize = 16;
const INET6_ADDRSTRLEN: usize = 46;
unsafe extern "C" {
    fn inet_ntop(
        af: i32,
        src: *const libc::c_void,
        dst: *mut libc::c_char,
        size: socklen_t,
    ) -> *const libc::c_char;
}
fn get_client_ip(fd: RawFd) -> Option<String> {
    unsafe {
        let mut addr: sockaddr_storage = zeroed();
        let mut len = size_of::<sockaddr_storage>() as socklen_t;

        if getpeername(fd, &mut addr as *mut _ as *mut sockaddr, &mut len) != 0 {
            eprintln!("getpeername failed");
            return None;
        }

        let mut ip_buf = [0i8; INET6_ADDRSTRLEN];

        match addr.ss_family as i32 {
            AF_INET => {
                let addr_in: &sockaddr_in = &*(&addr as *const _ as *const sockaddr_in);
                let src = &addr_in.sin_addr as *const _ as *const libc::c_void;
                if inet_ntop(AF_INET, src, ip_buf.as_mut_ptr(), INET_ADDRSTRLEN as socklen_t).is_null() {
                    return None;
                }
            }
            AF_INET6 => {
                let addr_in6: &sockaddr_in6 = &*(&addr as *const _ as *const sockaddr_in6);
                let src = &addr_in6.sin6_addr as *const _ as *const libc::c_void;
                if inet_ntop(AF_INET6, src, ip_buf.as_mut_ptr(), INET6_ADDRSTRLEN as socklen_t).is_null() {
                    return None;
                }
            }
            _ => return None,
        }

        Some(CStr::from_ptr(ip_buf.as_ptr()).to_string_lossy().into_owned())
    }
}

fn modify_headers(mut request: Request<Vec<u8>>, fd: i32, server: [u8; 10]) -> Request<Vec<u8>> {
    let mut host = String::new();
    for i in 0..3 {
        host.push_str(&server[i].to_string());
        host.push('.');
    }
    host.push_str(&server[3].to_string());
    host.push(':');
    host.push_str(&(((server[4] as i32) << 8) | (server[5] as i32)).to_string());
    request.headers_mut().insert(
        HeaderName::from_static("host"),
        HeaderValue::from_str(&host).unwrap(),
    );
    if let Some(client_ip) = get_client_ip(fd) {
        if let Ok(header_value) = HeaderValue::from_str(&client_ip) {
            request.headers_mut().insert(
                HeaderName::from_static("x-forwarded-for"),
                header_value,
            );
        }
    }

    request
}

fn parse_http_request(buffer: [u8; 1024]) -> Result<Request<Vec<u8>>, Box<dyn std::error::Error>> {
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut req = HttpParseRequest::new(&mut headers);

    let status = req.parse(&buffer)?;

    let parsed_len = match status {
        Status::Complete(len) => len,
        Status::Partial => return Err("Incomplete HTTP request".into()),
    };

    let method = req.method.ok_or("Missing method")?;
    let path = req.path.ok_or("Missing path")?;
    let version = match req.version {
        Some(1) => Version::HTTP_11,
        Some(0) => Version::HTTP_10,
        _ => return Err("Unknown HTTP version".into()),
    };

    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .version(version);

    for header in req.headers.iter() {
        builder = builder.header(header.name, header.value);
    }

    let body = buffer[parsed_len..].to_vec();

    Ok(builder.body(body)?)
}

fn serialize_request(req: Request<Vec<u8>>) -> REQ {
    let mut buffer = [0u8; 1024];
    let mut vec = Vec::new();

    let request_line = format!(
        "{} {} {}\r\n",
        req.method(),
        req.uri(),
        match req.version() {
            Version::HTTP_10 => "HTTP/1.0",
            Version::HTTP_11 => "HTTP/1.1",
            _ => "HTTP/1.1",
        }
    );
    vec.extend_from_slice(request_line.as_bytes());

    for (name, value) in req.headers() {
        vec.extend_from_slice(name.as_str().as_bytes());
        vec.extend_from_slice(b": ");
        vec.extend_from_slice(value.as_bytes());
        vec.extend_from_slice(b"\r\n");
    }

    vec.extend_from_slice(b"\r\n");
    vec.extend_from_slice(&req.body());

    let copy_len = vec.len().min(1024);
    buffer[..copy_len].copy_from_slice(&vec[..copy_len]);
    let mut termination_len = 0;
    for i in 0..copy_len {
        if buffer[i] != 0 {
            buffer[i] = vec[i];
        } else {
            termination_len = i;
            break;
        }
    }
    print!("{} \n", termination_len);

    REQ { req_data: buffer, n: termination_len }
}

fn when_identity_equals_conn_db_sock_fd(
    conn_db_sock_fd: i32,
    req_map: &mut HashMap<RawFd, REQ>,
    server_client_mapping: *mut HashMap<RawFd, RawFd>,
    kq: i32,
    addr: sockaddr_un,
    addr_len: u32,
    fd_ip_mapping: *mut HashMap<RawFd, [u8; 6]>,
    server_reqs_mapping: *mut HashMap<[u8; 6], HashSet<RawFd>>,
    conn_db_res_counter: &mut i32
) {
    unsafe {
        *conn_db_res_counter += 1;
        let mut buf = [0u8; 10];
        let n = read(conn_db_sock_fd, buf.as_mut_ptr() as *mut _, 10);
        if n > 0 {
            let client_fd = RawFd::from_be_bytes(buf[6..10].try_into().unwrap());
            let request = *req_map.get(&client_fd).unwrap();
            // println!("{:?}.{:?}.{:?}.{:?}:{:?}.{:?}", buf[0], buf[1], buf[2], buf[3], buf[4], buf[5]);
            let backend_services_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
            if backend_services_fd < 0 {
                panic!("Failed to create socket");
            }

            let ip = Ipv4Addr::new(buf[0], buf[1], buf[2], buf[3]);
            let sockaddr_in = sockaddr_in {
                sin_len: mem::size_of::<sockaddr_in>() as u8,
                sin_family: AF_INET as u8,
                sin_port: u16::to_be(((buf[4] as u16) << 8) | (buf[5] as u16)),
                sin_addr: in_addr {
                    s_addr: u32::from(ip).to_be(),
                },
                sin_zero: [0; 8],
            };

            let sockaddr_ptr = &sockaddr_in as *const sockaddr_in as *const sockaddr;

            let ret = connect(
                backend_services_fd,
                sockaddr_ptr,
                mem::size_of::<sockaddr_in>() as u32,
            );
            if ret < 0 {
                del_fd_to_kqueue(kq, backend_services_fd as usize);
                close(backend_services_fd);
                
                let mut conn_db_request = [0u8; 7];
                conn_db_request[0] = 2;
                for i in 1..7 {
                    conn_db_request[i] = buf[i-1];
                }

                let db_conn_status = write(
                    conn_db_sock_fd,
                    conn_db_request.as_ptr() as *const _,
                    conn_db_request.len(),
                );
                if db_conn_status < 0 {
                    if connect(
                        conn_db_sock_fd,
                        &addr as *const _ as *const sockaddr,
                        addr_len,
                    ) < 0
                    {
                        panic!("connect failed");
                    }

                    // del_fd_to_kqueue(&kq, conn_db_sock_fd as usize);
                    add_fd_to_kqueue(kq, conn_db_sock_fd as usize);

                    write(
                        conn_db_sock_fd,
                        conn_db_request.as_ptr() as *const _,
                        conn_db_request.len(),
                    );
                }

                let mut request_bytes = [0u8; 7];
                request_bytes[3..7].copy_from_slice(&client_fd.to_be_bytes());
                write(
                    conn_db_sock_fd,
                    request_bytes.as_ptr() as *const _,
                    request_bytes.len(),
                );
                let server: [u8; 6] = buf[..6].try_into().unwrap();
                if let Some(fdi_set) = (*server_reqs_mapping).get(&server) {
                    for &fdi in fdi_set {
                        let mut request_bytes_i = [0u8; 7];
                        request_bytes_i[3..7].copy_from_slice(&fdi.to_be_bytes());
                        write(
                            conn_db_sock_fd,
                            request_bytes_i.as_ptr() as *const _,
                            request_bytes_i.len(),
                        );
                    }
                }
            }

            
            let modified_request = serialize_request(modify_headers(parse_http_request(request.req_data).unwrap(), client_fd, buf));
            // println!("{:?}", &modified_request.req_data[..modified_request.n]);
            let write_ret = write(
                backend_services_fd,
                modified_request.req_data.as_ptr() as *const _,
                modified_request.n,
            );
            if write_ret < 0 {
                close(backend_services_fd);
                panic!("Failed to write to socket");
            }
            (*server_client_mapping).insert(backend_services_fd, client_fd);
            (*fd_ip_mapping).insert(backend_services_fd, buf[..6].try_into().unwrap());
            let server: [u8; 6] = buf[..6].try_into().unwrap();
            (*server_reqs_mapping)
                .entry(server)
                .or_insert_with(HashSet::new)
                .insert(client_fd);
            add_fd_to_kqueue(kq, backend_services_fd as usize);

            // write(front_req.client_fd, buf.as_ptr() as *const _, 1024);
            // close(front_req.client_fd);
            // del_fd_to_kqueue(kq, front_req.client_fd as usize);
            // server_client_mapping.remove(&front_req.client_fd);

            // del_fd_to_kqueue(kq, backend_services_fd as usize);
        } else {
            if connect(
                conn_db_sock_fd,
                &addr as *const _ as *const sockaddr,
                addr_len,
            ) < 0
            {
                panic!("connect failed");
            }
        }
    }
}

fn when_identity_else(
    client_fd: i32,
    conn_db_sock_fd: i32,
    req_map: &mut HashMap<RawFd, REQ>,
    server_client_mapping: *mut HashMap<RawFd, RawFd>,
    kq: i32,
    buf: [u8; 1024],
    n: usize,
    addr: sockaddr_un,
    addr_len: u32,
    fd_ip_mapping: *mut HashMap<RawFd, [u8; 6]>,
    server_reqs_mapping: *mut HashMap<[u8; 6], HashSet<RawFd>>,
    server_counter: &mut i32,
    client_counter: &mut i32
) {
    unsafe {
        match (*server_client_mapping).get(&client_fd) {
            Some(target_fd) => {
                // println!("\n\n{:?}", buf);
                // println!("b");
                *server_counter += 1;
                write(*target_fd, buf[..n].as_ptr() as *const _, n);
                del_fd_to_kqueue(kq, *target_fd as usize);
                (*server_client_mapping).remove(&target_fd);
                if let Some(server_key) = (*fd_ip_mapping).get(&client_fd) {
                    if let Some(fd_set) = (*server_reqs_mapping).get_mut(server_key) {
                        fd_set.remove(&target_fd);
                    }
                }
                close(*target_fd);

                let mut conn_db_request = [1u8; 7];
                let server = (*fd_ip_mapping).get(&client_fd).unwrap_or(&[0u8; 6]);
                for i in 1..7 {
                    conn_db_request[i] = server[i-1];
                }

                let db_conn_status = write(
                    conn_db_sock_fd,
                    conn_db_request.as_ptr() as *const _,
                    conn_db_request.len(),
                );
                if db_conn_status < 0 {
                    if connect(
                        conn_db_sock_fd,
                        &addr as *const _ as *const sockaddr,
                        addr_len,
                    ) < 0
                    {
                        panic!("connect failed");
                    }

                    // del_fd_to_kqueue(&kq, conn_db_sock_fd as usize);
                    add_fd_to_kqueue(kq, conn_db_sock_fd as usize);

                    write(
                        conn_db_sock_fd,
                        conn_db_request.as_ptr() as *const _,
                        conn_db_request.len(),
                    );
                }
                
                (*fd_ip_mapping).remove(&client_fd);
                del_fd_to_kqueue(kq, client_fd as usize);
                close(client_fd);
            }
            None => {
                // println!("{:?}", buf);
                // println!("a");
                *client_counter += 1;
                let mut request_bytes = [0u8; 7];
                request_bytes[3..7].copy_from_slice(&client_fd.to_be_bytes());
                let db_conn_status = write(
                    conn_db_sock_fd,
                    request_bytes.as_ptr() as *const _,
                    request_bytes.len(),
                );
                if db_conn_status < 0 {
                    if connect(
                        conn_db_sock_fd,
                        &addr as *const _ as *const sockaddr,
                        addr_len,
                    ) < 0
                    {
                        panic!("connect failed");
                    }

                    // del_fd_to_kqueue(&kq, conn_db_sock_fd as usize);
                    add_fd_to_kqueue(kq, conn_db_sock_fd as usize);

                    write(
                        conn_db_sock_fd,
                        request_bytes.as_ptr() as *const _,
                        request_bytes.len(),
                    );
                }

                req_map.insert(client_fd, REQ { req_data: buf, n: n });
            }
        }
    }
}

#[derive(Clone, Copy)]
struct REQ {
    req_data: [u8; 1024],
    n: usize
}

pub fn worker_loop(sock_fd: i32) {
    let mut req_maps: HashMap<RawFd, REQ> = HashMap::new();
    let mut server_client_mapping: HashMap<RawFd, RawFd> = HashMap::new();
    let mut fd_ip_mapping: HashMap<RawFd, [u8; 6]> = HashMap::new();
    let mut server_req_mapping: HashMap<[u8; 6], HashSet<RawFd>> = HashMap::new();
    let mut server_backend_fd_mapping: HashMap<[u8; 6], Vec<RawFd>> = HashMap::new();

    let mut client_counter = 0;
    let mut server_counter = 0;
    let mut conn_db_res_counter = 0;

    unsafe {
        let conn_db_sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if conn_db_sock_fd < 0 {
            panic!("socket creation failed");
        }

        let mut addr: sockaddr_un = mem::zeroed();
        addr.sun_family = AF_UNIX as sa_family_t;
        let path = CString::new(SOCK_PATH).unwrap();
        ptr::copy_nonoverlapping(
            path.as_ptr(),
            addr.sun_path.as_mut_ptr() as *mut i8,
            path.as_bytes().len(),
        );
        let addr_len = (std::mem::size_of::<sa_family_t>() + path.as_bytes().len()) as u32;

        let kq = kqueue();
        add_fd_to_kqueue(kq, sock_fd as usize);

        loop {
            let mut events: [kevent; 32] = zeroed();
            let nev = kevent(kq, ptr::null(), 0, events.as_mut_ptr(), 32, ptr::null());
            if nev < 0 {
                eprintln!("kevent error");
                break;
            }

            for i in 0..nev {
                let ev = events[i as usize];
                if ev.filter == EVFILT_READ {
                    if ev.ident == sock_fd as usize {
                        let client_fd = accept(sock_fd, ptr::null_mut(), ptr::null_mut());
                        if client_fd >= 0 {
                            add_fd_to_kqueue(kq, client_fd as usize);
                        }
                    } else if ev.ident == conn_db_sock_fd as usize {
                        when_identity_equals_conn_db_sock_fd(
                            conn_db_sock_fd,
                            &mut req_maps,
                            &mut server_client_mapping,
                            kq,
                            addr,
                            addr_len,
                            &mut fd_ip_mapping,
                            &mut server_req_mapping,
                            &mut conn_db_res_counter
                        );
                    } else {
                        let client_fd = ev.ident as i32;

                        if client_fd >= 0 {
                            let mut buf = [0u8; 1024];
                            let n = read(client_fd, buf.as_mut_ptr() as *mut _, 1024);
                            // println!("message from: {} by {}", client_fd, std::process::id());
                            // println!("{:?} {}", buf, n);
                            if n > 0 {
                                when_identity_else(
                                    client_fd,
                                    conn_db_sock_fd,
                                    &mut req_maps,
                                    &mut server_client_mapping,
                                    kq,
                                    buf,
                                    n as usize,
                                    addr,
                                    addr_len,
                                    &mut fd_ip_mapping,
                                    &mut server_req_mapping,
                                    &mut server_counter,
                                    &mut client_counter
                                );
                            } else {
                                // println!("{}, {}, {}", server_counter, client_counter, conn_db_res_counter);
                                // println!("{}", req_maps.len());
                                del_fd_to_kqueue(kq, client_fd as usize);
                                close(client_fd);
                            }
                        }
                    }
                }
            }
        }
        close(kq);
        del_fd_to_kqueue(kq, sock_fd as usize);
    }
}
