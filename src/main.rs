use std::time::Duration;

use tokio::{
    net::UdpSocket,
    time::{self},
};

use networking::{Connection, Message, PORTS_LIST};

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

    let (from_app, mut to_app) = Connection::create_connection(socket, peer_ip);

    tokio::spawn(async move {
        loop {
            if let Some(message) = to_app.recv().await {
                println!("Received message: {:?}", message);
            }
        }
    });

    let from_app_stdin = from_app.clone();

    tokio::spawn(async move {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let mut stdin = BufReader::new(tokio::io::stdin());
        let mut line = String::new();

        while stdin.read_line(&mut line).await.unwrap_or(0) > 0 {
            let _ = from_app_stdin.send(Message::Chat(line.clone())).await;
            line.clear();
        }
    });

    let _ = tokio::signal::ctrl_c().await;

    let _ = from_app.send(Message::DropConnection).await;

    time::sleep(Duration::from_millis(100)).await;
}
