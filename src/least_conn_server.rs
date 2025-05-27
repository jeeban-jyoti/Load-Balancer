use std::{collections::HashMap, ptr::null_mut};

#[derive(Debug)]
struct DataNode {
    server: [u8; 6],
    next: *mut DataNode,
    prev: *mut DataNode,
    head: *mut ConnNode
}

impl DataNode {
    fn new() -> *mut DataNode {
        let data_node = Box::new(DataNode {
            server: [0u8; 6],
            next: null_mut(),
            prev: null_mut(),
            head: null_mut()
        });

        Box::into_raw(data_node)
    }
}

#[derive(Debug)]
struct ConnNode {
    conns: u32,
    prev: *mut ConnNode,
    next: *mut ConnNode,
    chain_head: *mut DataNode,
    chain_tail: *mut DataNode
}

impl ConnNode {
    fn new() -> *mut ConnNode {
        let chain_head_node = DataNode::new();
        let chain_tail_node = DataNode::new();
        unsafe {
            (&mut *chain_head_node).next = chain_tail_node;
            (&mut *chain_tail_node).prev = chain_head_node;
        }
        let conn_node = Box::new(ConnNode {
            conns: 0,
            prev: null_mut(),
            next: null_mut(),
            chain_head: chain_head_node,
            chain_tail: chain_tail_node
        });
        let conn_node_ptr = Box::into_raw(conn_node);
        unsafe {
            (&mut *chain_head_node).head = conn_node_ptr;
            (&mut *chain_tail_node).head = conn_node_ptr;
        }
        conn_node_ptr
    }
}

pub struct LCS {
    head: *mut ConnNode,
    tail: *mut ConnNode,
    conn_count_map: HashMap<u32, *mut ConnNode>,
    server_node_map: HashMap<[u8; 6], *mut DataNode>
}

impl LCS {
    pub fn new() -> LCS {
        unsafe {
            let head = ConnNode::new();
            let tail = ConnNode::new();
    
            (*head).next = tail;
            (*tail).prev = head;
    
            LCS {
                head,
                tail,
                conn_count_map: HashMap::new(),
                server_node_map: HashMap::new(),
            }
        }
    }

    pub fn insert(&mut self, server: &[u8; 6]) -> Result<(), &'static str> {
        unsafe {
            if !self.conn_count_map.contains_key(&0) {
                let conn_node = &mut *ConnNode::new();
                let h = &mut *self.head;
                let n = &mut *h.next;

                h.next = conn_node;
                n.prev = conn_node;
                conn_node.next = n;
                conn_node.prev = h;

                conn_node.conns = 0;

                self.conn_count_map.insert(0, conn_node);
            }

            let conn_node = (&mut *self.head).next;
            let data_node = &mut *DataNode::new();
            
            let h = &mut *(&mut *conn_node).chain_head;
            let n = &mut *(&mut *h).next;
            
            h.next = data_node;
            n.prev = data_node;
            data_node.next = n;
            data_node.prev = h;

            self.server_node_map.insert(*server, data_node);
            data_node.head = conn_node;
            data_node.server = *server;
        }
        Ok(())
    }

    pub fn _delete(&mut self, server: &[u8; 6]) -> Result<(), &'static str> {
        unsafe {
            let data_node = *self.server_node_map.get(server).ok_or("Key not found")?;
            let p = (&mut *data_node).prev;
            let n = (&mut *data_node).next;

            (&mut *p).next = n;
            (&mut *n).prev = p;

            self.server_node_map.remove(server);
            let _ = Box::from_raw(data_node);

            if (&mut *p).head == null_mut() && (&mut *n).head == null_mut() {
                let conn_head = (&mut *p).head;
                let cp = (&mut *conn_head).prev;
                let cn = (&mut *conn_head).next;

                (&mut *cp).next = cn;
                (&mut *cn).prev = cp;
                
                self.conn_count_map.remove(&(&mut *conn_head).conns);
                let _ = Box::from_raw(p);
                let _ = Box::from_raw(n);
                let _ = Box::from_raw(conn_head);
            }
        }
        Ok(())
    }

    pub fn get_least_conn_server(&self) -> Result<[u8; 6], &'static str>{
        unsafe {
            let head = &mut *self.head;
            if head.next == self.tail {
                return Err("No servers exist");
            }
            let chain_head = &mut *(&mut *(head.next)).chain_head;
            if chain_head.next == &mut *(&mut *(head.next)).chain_tail {
                return Err("No servers exist");
            }

            let server = (&mut *chain_head.next).server;

            return Ok(server);
        }
    }

    pub fn get_stats(&self) -> Result<Vec<([u8; 6], u32)>, &'static str> {
        let mut stats: Vec<([u8; 6], u32)> = Vec::new();
        unsafe {
            for (_, val) in self.server_node_map.iter() {
                let server = (&mut *(*val)).server.clone();
                let conns = (&mut *((&mut *(*val)).head)).conns;

                stats.push((server, conns));
            }
        }
        Ok(stats)
    }

    pub fn server_conn_increament(&mut self, server: &[u8; 6]) -> Result<(), &'static str> {
        unsafe {
            let data_node = *self.server_node_map.get(server).ok_or("Server not found")?;
            let curr_conn_node = &mut *(*data_node).head;
            let new_conns = curr_conn_node.conns + 1;
    
            let p = (*data_node).prev;
            let n = (*data_node).next;
            (*p).next = n;
            (*n).prev = p;
            
            // println!("prev : {:?}", *p);
            // println!("next : {:?}", *n);
            // println!();
            // println!();
            let new_conn_node = if let Some(&node) = self.conn_count_map.get(&new_conns) {
                &mut *node
            } else {
                let node = ConnNode::new();
                self.conn_count_map.insert(new_conns, node);
                let h = (&mut *data_node).head;
                let n = (&mut *h).next;
                
                // self.traverse();
                (*node).next = n;
                (*node).prev = h;
                (&mut *h).next = node;
                (&mut *n).prev = node;
                
                &mut *node
            };

            if (&mut *p).prev == null_mut() && (&mut *n).next == null_mut() {
                let conn_head = (&mut *p).head;
                let cp = (&mut *conn_head).prev;
                let cn = (&mut *conn_head).next;
                // self.traverse();
                
                (&mut *cp).next = cn;
                (&mut *cn).prev = cp;
                
                self.conn_count_map.remove(&(&mut *conn_head).conns);
                let _ = Box::from_raw(p);
                let _ = Box::from_raw(n);
                let _ = Box::from_raw(conn_head);
            }
    
            let h = new_conn_node.chain_head;
            let n = (&mut *h).next;
    
            (&mut *data_node).prev = h;
            (&mut *data_node).next = n;
            (&mut *h).next = data_node;
            (&mut *n).prev = data_node;
            (&mut *data_node).head = new_conn_node;
    
            new_conn_node.conns = new_conns;
        }
        Ok(())
    }

    pub fn server_conn_decreament(&mut self, server: &[u8; 6]) -> Result<(), &'static str> {
        unsafe {
            let data_node = *self.server_node_map.get(server).ok_or("Server not found")?;
            let curr_conn_node = &mut *(*data_node).head;
            if curr_conn_node.conns == 0 {
                return Err("already 0 connections");
            }
            let new_conns = curr_conn_node.conns - 1;
    
            let p = (*data_node).prev;
            let n = (*data_node).next;
            (*p).next = n;
            (*n).prev = p;

            let new_conn_node = if let Some(&node) = self.conn_count_map.get(&new_conns) {
                &mut *node
            } else {
                let node = ConnNode::new();
                self.conn_count_map.insert(new_conns, node);
                let h = (&mut *data_node).head;
                let p = (&mut *h).prev;
    
                (*node).next = h;
                (*node).prev = p;
                (&mut *h).prev = node;
                (&mut *p).next = node;
    
                &mut *node
            };

            if (&mut *p).prev == null_mut() && (&mut *n).next == null_mut() {
                let conn_head = (&mut *p).head;
                let cp = (&mut *conn_head).prev;
                let cn = (&mut *conn_head).next;

                (&mut *cp).next = cn;
                (&mut *cn).prev = cp;
                
                self.conn_count_map.remove(&(&mut *conn_head).conns);
                let _ = Box::from_raw(p);
                let _ = Box::from_raw(n);
                let _ = Box::from_raw(conn_head);
            }
    
            let h = &mut *new_conn_node.chain_head;
            let n = &mut *h.next;
    
            (*data_node).prev = h;
            (*data_node).next = n;
            h.next = data_node;
            n.prev = data_node;
            (*data_node).head = new_conn_node;
    
            new_conn_node.conns = new_conns;
        }
        // self.traverse();
        Ok(())
    }

    fn _traverse(&self) {
        unsafe {
            let mut ptr = self.head;
            while ptr != null_mut() {
                let mut ptr2 = (&mut *ptr).chain_head;
                println!("{:?}", *ptr);
                while ptr2 != null_mut() {
                    print!("{:?}", *ptr2);
                    ptr2 = (&mut *ptr2).next;
                }
                ptr = (&mut *ptr).next;
                println!();
            }
        }
    }
}