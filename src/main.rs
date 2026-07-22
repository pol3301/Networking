use std::{net::SocketAddr, time::Duration};

use tokio::{
    net::UdpSocket,
    time::{self, interval},
};

async fn punch_hole(socket: &UdpSocket, peer_addr: SocketAddr) -> Result<(), &'static str> {
    let mut ticker = interval(Duration::from_millis(200));
    let mut buf = [0; 1024];

    let punch: &[u8] = b"PUNCH";
    let ack: &[u8] = b"ACK";

    let mut current_strategy = punch;

    let punch_attempt = time::timeout(Duration::from_mins(1), async {
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let _ = socket.send_to(current_strategy, peer_addr).await;
                }

                result = socket.recv_from(&mut buf) => {
                    if let Ok((len, addr)) = result &&
                        addr == peer_addr {

                        match &buf[..len] {
                            b"PUNCH" => {
                                current_strategy = ack;
                                _ = socket.send_to(current_strategy, peer_addr)
                            },

                            b"ACK" => {
                                for _ in 0..3 {
                                    let _ = socket.send_to(b"ACK", peer_addr).await;
                                }

                                return;
                            },
                            _ => {},
                        }
                    }
                }
            }
        }
    })
    .await;

    match punch_attempt {
        Ok(_) => Ok(()),
        Err(_) => Err("Failed to punch a hole in the given time"),
    }
}

const PORTS_LIST: [&str; 5] = ["32432", "24325", "24377", "25379", "36727"];

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 3 {
        eprintln!("Wrong usage: ./program peer_addr port");
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

    let peer_port = args[2]
        .parse::<u16>()
        .unwrap_or_else(|_| panic!("Could not parse port {}", args[2]));

    let peer_addr = SocketAddr::new(peer_ip, peer_port);

    match punch_hole(&socket, peer_addr).await {
        Ok(_) => println!("Punched a hole!"),
        Err(e) => eprintln!("{}", e),
    }
}
