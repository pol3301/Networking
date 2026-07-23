use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use tokio::{
    net::UdpSocket,
    time::{self, interval},
};

const PORTS_LIST: [&str; 5] = ["32432", "24325", "24377", "25379", "36727"];

async fn punch_hole(socket: &UdpSocket, peer_ip: IpAddr) -> Result<SocketAddr, &'static str> {
    let mut ticker = interval(Duration::from_millis(200));
    let mut buf = [0; 1024];

    let punch: &[u8] = b"PUNCH";
    let ack: &[u8] = b"ACK";

    let peer_addr_list: [SocketAddr; 5] =
        PORTS_LIST.map(|port| SocketAddr::new(peer_ip, port.parse::<u16>().unwrap()));

    let mut current_strategy = punch;

    let mut peer_addr: Option<SocketAddr> = None;

    let punch_attempt = time::timeout(Duration::from_mins(1), async {
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                match peer_addr {
                    Some(addr) => {let _ = socket.send_to(current_strategy, addr).await;},
                    None => {
                        for addr in peer_addr_list {
                            let _ = socket.send_to(current_strategy, addr).await;
                        }
                    },
                }
                }

                result = socket.recv_from(&mut buf) => {
                    if let Ok((len, connection_addr)) = result {
                        if let Some(addr) = peer_addr {
                            if addr == connection_addr {
                                peer_addr = Some(addr);
                                match &buf[..len] {
                                    b"PUNCH" => {
                                        current_strategy = ack;
                                        let _ = socket.send_to(current_strategy, addr).await;
                                    },

                                    b"ACK" => {
                                        for _ in 0..3 {
                                            let _ = socket.send_to(b"ACK", addr).await;
                                        }

                                        return peer_addr.unwrap();
                                    },
                                    _ => {},
                                }
                            }
                        } else {
                            for addr in peer_addr_list {
                                if addr == connection_addr {
                                    peer_addr = Some(addr);
                                    match &buf[..len] {
                                        b"PUNCH" => {
                                            current_strategy = ack;
                                            let _ = socket.send_to(current_strategy, addr).await;
                                        },

                                        b"ACK" => {
                                            for _ in 0..3 {
                                                let _ = socket.send_to(b"ACK", addr).await;
                                            }

                                            return peer_addr.unwrap();
                                        },
                                        _ => {},
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    })
    .await;

    match punch_attempt {
        Ok(peer_addr) => Ok(peer_addr),
        Err(_) => Err("Failed to punch a hole in the given time"),
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 2 {
        eprintln!("Wrong usage: ./program peer_addr");
        return;
    }

    let mut socket = None;

    for port in PORTS_LIST {
        if let Ok(sock) = UdpSocket::bind(format!("0.0.0.0:{}", port)).await {
            socket = Some(sock);
            break;
        }
    }

    let socket = socket.expect("Could not bind to any of the listed ports");

    println!("Socket: {}", socket.local_addr().unwrap());

    let peer_ip = args[1]
        .parse::<std::net::IpAddr>()
        .unwrap_or_else(|_| panic!("Could not parse address {}", args[1]));

    match punch_hole(&socket, peer_ip).await {
        Ok(addr) => println!("Punched a hole! Peer's port is: {}", addr.port()),
        Err(e) => eprintln!("{}", e),
    }
}
