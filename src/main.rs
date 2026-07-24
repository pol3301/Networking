use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::{
    net::UdpSocket,
    sync::mpsc::{Receiver, Sender},
    time::{self, Instant, interval},
};

pub const PORTS_LIST: [&str; 5] = ["32432", "24325", "24377", "25379", "36727"];

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum Message {
    Move(u16),
    Chat(String),
    KeepAlive,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Packet {
    pub sequence_id: u32,
    pub payload: Message,

    pub ack_base_id: u32,
    pub ack_bitfield: u32,
}

pub struct PacketPending {
    pub packet: Packet,
    pub tries: u32,
    pub last_tried: Instant,
}

impl PacketPending {
    pub fn new(packet: Packet) -> Self {
        PacketPending {
            packet,
            tries: 0,
            last_tried: Instant::now(),
        }
    }
}

pub struct ConnectionStatus {
    pub unacked_queue: BTreeMap<u32, PacketPending>,
    pub out_of_order_queue: BTreeMap<u32, Packet>,

    pub curr_received_id_them: u32,
    pub curr_processed_id_them: u32,
    pub ack_bitfield_them: u32,

    pub curr_id_us: u32,
    pub last_contact: Instant,
}

pub async fn punch_hole(socket: &UdpSocket, peer_ip: IpAddr) -> Result<SocketAddr, &'static str> {
    let mut ticker = interval(Duration::from_millis(200));
    let mut buf = [0; 1024];

    let punch: &[u8] = b"PUNCH";
    let ack: &[u8] = b"ACK";

    let peer_addr_list: [SocketAddr; 5] =
        PORTS_LIST.map(|port| SocketAddr::new(peer_ip, port.parse::<u16>().unwrap()));

    let mut peer_addr: Option<SocketAddr> = None;

    let punch_attempt = time::timeout(Duration::from_mins(1), async {
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                match peer_addr {
                    Some(addr) => {let _ = socket.send_to(punch, addr).await;},
                    None => {
                        for addr in peer_addr_list {
                            let _ = socket.send_to(ack, addr).await;
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
                                        let _ = socket.send_to(ack, addr).await;
                                    },

                                    b"ACK" => {
                                        for _ in 0..3 {
                                            let _ = socket.send_to(ack, addr).await;
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
                                            let _ = socket.send_to(ack, addr).await;
                                        },

                                        b"ACK" => {
                                            for _ in 0..3 {
                                                let _ = socket.send_to(ack, addr).await;
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

impl Default for ConnectionStatus {
    fn default() -> Self {
        Self {
            unacked_queue: Default::default(),
            out_of_order_queue: Default::default(),
            curr_received_id_them: 0,
            ack_bitfield_them: 0,
            curr_processed_id_them: 0,
            curr_id_us: 0,
            last_contact: Instant::now(),
        }
    }
}

pub struct Connection {
    status: ConnectionStatus,
    socket: UdpSocket,
    peer: SocketAddr,
    from_app: tokio::sync::mpsc::Receiver<Message>,
    to_app: tokio::sync::mpsc::Sender<Message>,
}

impl Connection {
    pub fn new(
        socket: UdpSocket,
        peer: SocketAddr,
        from_app: Receiver<Message>,
        to_app: Sender<Message>,
    ) -> Self {
        Connection {
            status: ConnectionStatus::default(),
            socket,
            peer,
            from_app,
            to_app,
        }
    }

    pub fn process_ack(&mut self, ack: u32, ack_bitfield: u32) {
        self.status.unacked_queue.remove(&ack);

        if !self.status.unacked_queue.is_empty() {
            let mut bits = ack_bitfield;
            while bits != 0 {
                self.status
                    .unacked_queue
                    .remove(&(ack - bits.trailing_zeros()));

                bits = (bits - 1) & bits;
            }
        }
    }

    pub async fn process_packet(&mut self, packet: Packet) {
        println!("Processing packet: {:?}", packet);

        self.status.last_contact = Instant::now();

        self.process_ack(packet.ack_base_id, packet.ack_bitfield);

        if packet.payload == Message::KeepAlive {
            return;
        }

        let expected_id = self.status.curr_processed_id_them + 1;

        if packet.sequence_id > self.status.curr_received_id_them {
            let diff = packet.sequence_id - self.status.curr_received_id_them;

            if diff >= 32 {
                self.status.ack_bitfield_them = 0
            } else {
                self.status.ack_bitfield_them <<= diff;
            }

            for i in 0..std::cmp::min(diff, 32) {
                if self
                    .status
                    .out_of_order_queue
                    .contains_key(&(packet.sequence_id - i))
                {
                    self.status.ack_bitfield_them |= 1 << i;
                }
            }

            self.status.curr_received_id_them = packet.sequence_id;
        } else if packet.sequence_id < self.status.curr_received_id_them {
            let diff = self.status.curr_received_id_them - packet.sequence_id;

            if diff < 32 {
                self.status.ack_bitfield_them |= 1 << diff;
            }
        }

        if packet.sequence_id == expected_id {
            let _ = self.to_app.send(packet.payload).await;

            self.status.curr_processed_id_them += 1;

            let mut next_expected = expected_id + 1;

            while let Some(buffered_packet) = self.status.out_of_order_queue.remove(&next_expected)
            {
                let _ = self.to_app.send(buffered_packet.payload).await;
                self.status.curr_processed_id_them += 1;
                next_expected += 1;
            }
        } else if packet.sequence_id > expected_id {
            let _ = self
                .status
                .out_of_order_queue
                .insert(packet.sequence_id, packet);
        }
    }

    pub async fn send(&mut self, message: Message) {
        println!("Sending message:{:?}", message);

        self.status.curr_id_us += 1;

        let packet = Packet {
            sequence_id: self.status.curr_id_us,
            payload: message,
            ack_base_id: self.status.curr_received_id_them,
            ack_bitfield: self.status.ack_bitfield_them,
        };

        let bytes: Vec<u8> = bincode::serialize(&packet).unwrap();
        let _ = self.socket.send_to(&bytes, self.peer).await;

        let packet_pending = PacketPending {
            packet,
            tries: 0,
            last_tried: Instant::now(),
        };

        self.status
            .unacked_queue
            .insert(self.status.curr_id_us, packet_pending);
    }

    pub async fn main_loop(mut self) {
        let mut buf = [0; 1024];

        loop {
            tokio::select! {
                incoming_message = self.socket.recv_from(&mut buf) => {
                    if let Ok((len, from)) = incoming_message && from == self.peer {
                        match bincode::deserialize::<Packet>(&buf[..len]) {
                            Ok(packet) => {
                                self.process_packet(packet).await;
                        },
                            Err(e) => eprintln!("Failed to parse packet: {}", e),
                        }
                    }
                }

                outgoing_message = self.from_app.recv() => {
                    if let Some(message) = outgoing_message {
                        self.send(message).await;
                    }
                }
            }
        }
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

    let (from_app_tx, from_app_rx) = tokio::sync::mpsc::channel(1);
    let (to_app_tx, mut to_app_rx) = tokio::sync::mpsc::channel(1);

    match punch_hole(&socket, peer_ip).await {
        Ok(addr) => {
            println!("Punched a hole! Peer's port is: {}", addr.port());
            let connection = Connection::new(socket, addr, from_app_rx, to_app_tx);
            tokio::spawn(connection.main_loop());
        }
        Err(e) => {
            eprintln!("{}", e);
            return;
        }
    }

    tokio::spawn(async move {
        loop {
            if let Some(message) = to_app_rx.recv().await {
                println!("Received message: {:?}", message);
            }
        }
    });

    let message = Message::Chat("Hello world!".to_owned());

    let _ = from_app_tx.send(message).await;

    let _ = tokio::signal::ctrl_c().await;
}
