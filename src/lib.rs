use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::{
    net::UdpSocket,
    sync::mpsc::{Receiver, Sender, channel},
    time::{self, Instant, interval},
};

macro_rules! debug_print {
    ($($arg:tt)*) => {
        if cfg!(debug_assertions) {
            println!($($arg)*);
        }
    };
}

pub const PORTS_LIST: [&str; 5] = ["32432", "24325", "24377", "25379", "36727"];

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum Message {
    Move(u16),
    Chat(String),
    EstablishedConnection,
    DropConnection,
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

pub struct ConnectionStatus {
    pub unacked_queue: BTreeMap<u32, PacketPending>,
    pub out_of_order_queue: BTreeMap<u32, Packet>,

    pub curr_received_id_them: u32,
    pub curr_processed_id_them: u32,
    pub ack_bitfield_them: u32,

    pub curr_id_us: u32,
    pub last_contact_them: Instant,
    pub last_contact_us: Instant,
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
                            let _ = socket.send_to(punch, addr).await;
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
            last_contact_them: Instant::now(),
            last_contact_us: Instant::now(),
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

    pub fn create_connection(
        socket: UdpSocket,
        peer: IpAddr,
    ) -> (Sender<Message>, Receiver<Message>) {
        let (tx_app, rx_us) = channel(32);
        let (tx_us, rx_app) = channel(32);

        tokio::spawn(async move {
            if let Ok(peer_addr) = punch_hole(&socket, peer).await {
                let connection = Self::new(socket, peer_addr, rx_us, tx_us);
                connection.main_loop().await;
            } else {
                eprintln!("Couldn't establish connection to {}", peer);
            }
        });

        (tx_app, rx_app)
    }

    pub fn process_ack(&mut self, ack: u32, ack_bitfield: u32) {
        self.status.unacked_queue.remove(&ack);

        if !self.status.unacked_queue.is_empty() {
            let mut bits = ack_bitfield;
            while bits != 0 {
                let tz = bits.trailing_zeros();

                self.status
                    .unacked_queue
                    .remove(&(ack.wrapping_sub(tz + 1)));

                bits = (bits - 1) & bits;
            }
        }
    }

    pub async fn drop_connection(&mut self) {
        let _ = self.to_app.send(Message::DropConnection).await;
        self.send(Message::DropConnection).await;
    }

    pub async fn process_packet(&mut self, packet: Packet) {
        self.status.last_contact_them = Instant::now();

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
                self.status.ack_bitfield_them |= 1 << (diff - 1);
            }

            self.status.curr_received_id_them = packet.sequence_id;
        } else if packet.sequence_id < self.status.curr_received_id_them {
            let diff = self.status.curr_received_id_them - packet.sequence_id;

            if diff <= 32 {
                self.status.ack_bitfield_them |= 1 << (diff - 1);
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
        let should_be_acked = message != Message::KeepAlive && message != Message::DropConnection;

        if should_be_acked {
            self.status.curr_id_us += 1;
        }

        let packet = Packet {
            sequence_id: self.status.curr_id_us,
            payload: message,
            ack_base_id: self.status.curr_received_id_them,
            ack_bitfield: self.status.ack_bitfield_them,
        };

        let bytes: Vec<u8> = bincode::serialize(&packet).unwrap();
        let _ = self.socket.send_to(&bytes, self.peer).await;
        self.status.last_contact_us = Instant::now();

        if should_be_acked {
            let packet_pending = PacketPending {
                packet,
                tries: 0,
                last_tried: Instant::now(),
            };

            self.status
                .unacked_queue
                .insert(self.status.curr_id_us, packet_pending);
        }
    }

    pub async fn main_loop(mut self) {
        let mut buf = [0; 1024];
        let mut ticker = interval(Duration::from_millis(50));

        let _ = self.to_app.send(Message::EstablishedConnection).await;

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if Instant::now() - self.status.last_contact_us >= Duration::from_secs(4) {
                        self.send(Message::KeepAlive).await;
                    }

                    if Instant::now() - self.status.last_contact_them >= Duration::from_secs(20) {
                        debug_print!("Dropping connection because of 20 second timeout");
                        self.drop_connection().await;
                        return;
                    }

                    let mut should_drop = false;

                    for unacked_packet in self.status.unacked_queue.values_mut() {
                        if Instant::now() - unacked_packet.last_tried >= Duration::from_millis(200) {
                            unacked_packet.tries += 1;
                            let bytes: Vec<u8> = bincode::serialize(&unacked_packet.packet).unwrap();
                            let _ = self.socket.send_to(&bytes, self.peer).await;

                            unacked_packet.last_tried = Instant::now();

                            if unacked_packet.tries >= 5 {
                                should_drop = true;
                                break;
                            }
                        }
                    };

                    if should_drop {
                        self.drop_connection().await;
                        debug_print!("Dropping connection because of unacked message");
                        return;
                    }
                }

                incoming_message = self.socket.recv_from(&mut buf) => {
                    if let Ok((len, from)) = incoming_message && from == self.peer {
                        match bincode::deserialize::<Packet>(&buf[..len]) {
                            Ok(packet) => {
                                if packet.payload == Message::DropConnection {
                                    self.drop_connection().await;
                                    debug_print!("Dropping connection because peer dropped connectio");
                                    return;
                                }
                                self.process_packet(packet).await;
                                self.send(Message::KeepAlive).await;
                        },
                            Err(e) => eprintln!("Failed to parse packet: {}", e),
                        }
                    }
                }

                outgoing_message = self.from_app.recv() => {
                    if let Some(message) = outgoing_message {
                        if message != Message::DropConnection {
                            self.send(message).await;
                        } else {
                            self.drop_connection().await;
                            debug_print!("Dropping connection because app sent drop connection message");
                            return;
                        }
                    } else {
                        self.drop_connection().await;
                        debug_print!("Dropping connection because app's channel was destroyed");
                        return;
                    }
                }
            }
        }
    }
}
