use std::{net::SocketAddr, time::Duration};

use tokio::{
    net::UdpSocket,
    time::{self, interval},
};

async fn punch_hole(socket: &UdpSocket, peer_addr: SocketAddr) -> Result<(), &'static str> {
    let mut ticker = interval(Duration::from_millis(200));
    let mut buf = [0; 1024];

    let punch = b"PUNCH";

    let punch_attempt = time::timeout(Duration::from_secs(15), async {
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let _ = socket.send_to(punch, peer_addr).await;
                }

                result = socket.recv_from(&mut buf) => {
                    if let Ok((_len, addr)) = result &&
                        addr == peer_addr {
                            return ;
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

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 3 {
        eprintln!("Wrong usage: ./program peer_addr port");
        return;
    }

    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();

    println!("Socket: {}", socket.local_addr().unwrap());

    let peer_ip = args[1]
        .parse::<std::net::IpAddr>()
        .unwrap_or_else(|_| panic!("Could not parse address {}", args[1]));

    let peer_port = args[2]
        .parse::<u16>()
        .unwrap_or_else(|_| panic!("Could not parse port {}", args[2]));

    let peer_addr = SocketAddr::new(peer_ip, peer_port);

    let _ = punch_hole(&socket, peer_addr).await;
}
