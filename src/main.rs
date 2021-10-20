//! MsgBus is a multi producer/multi consumer bus where every client message
//! is replicated to each other client.
use clap::{App, Arg};
use std::{sync::Arc, thread::JoinHandle};

use crossbeam_channel::{unbounded, Receiver, Sender, TryRecvError};

type Msg = Arc<String>;
type ClientId = usize;

struct MsgBus {
    // We have a sender for each client
    clients: Vec<Sender<Msg>>,

    // We have a single receiver of messages
    rx_control: Receiver<ControlMsg>,

    // This sender is cloned when creating a new client
    tx_control: Sender<ControlMsg>,
}

/// Client messages to MsgBus
enum ControlMsg {
    /// A message which shall be broadcasted to every other client.
    /// It includes the identity of the senter because we want
    /// to skip it when broadcasting the event.
    Msg(ClientId, Msg),
    /// Ask bus to shut down
    Exit,
}

impl MsgBus {
    fn new() -> Self {
        let clients = vec![];
        let (tx_control, rx_control) = unbounded();
        Self {
            tx_control,
            rx_control,
            clients,
        }
    }

    /// Create a new client connected to the bus
    fn add_client(&mut self) -> Client {
        let (tx, rx) = unbounded();
        let id = self.clients.len();
        self.clients.push(tx);
        Client {
            id,
            rx,
            tx_control: self.tx_control.clone(),
        }
    }

    /// Run MsgBus until a client decides to exit
    fn run(self) {
        log::info!("Starting bus");
        loop {
            match self.rx_control.recv() {
                // Exiting will drop self, which will drop the client senders, which
                // will shut down clients.
                Ok(ControlMsg::Exit) => break,
                // Broadcast messages to every other client
                Ok(ControlMsg::Msg(sender_id, msg)) => {
                    for (n, client) in self.clients.iter().enumerate() {
                        // Skip the sender
                        if n != sender_id {
                            // We don't care if a client prematurely exited
                            let _ = client.send(msg.clone());
                        }
                    }
                }
                // Since we keep a copy of tx_control, this can't happen:
                Err(_) => unreachable!(),
            }
        }
    }
}

struct Client {
    id: ClientId,
    rx: Receiver<Msg>,
    tx_control: Sender<ControlMsg>,
}

impl Client {
    fn run(self, max_messages: usize) {
        log::info!("{} - Starting", self.id);
        for message in 0..max_messages {
            // Handle pending messages
            loop {
                match self.rx.try_recv() {
                    Ok(msg) => log::info!("{} - received '{}'", self.id, msg),
                    Err(TryRecvError::Empty) => {
                        break;
                    }
                    Err(TryRecvError::Disconnected) => {
                        log::info!("{} - Exiting", self.id);
                        return;
                    }
                }
            }
            // Send a new message
            let msg = Arc::new(format!("Message #{}.{}", self.id, message));
            log::info!("{} - sending '{}'", self.id, msg);
            let _ = self.tx_control.send(ControlMsg::Msg(self.id, msg));
        }
        log::info!("{} - All messages sent. Requesting shut down", self.id);
        let _ = self.tx_control.send(ControlMsg::Exit);
    }
}

fn main() {
    env_logger::init();
    let matches = App::new("msgbus")
        .arg(
            Arg::with_name("num_clients")
                .help("Number of clients")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("max_messages")
                .help("Number of messages before a client sends the exit signal")
                .takes_value(true),
        )
        .get_matches();
    let num_clients = matches
        .value_of("num_clients")
        .unwrap_or("200")
        .parse()
        .expect("Number of clients must be a positive integer");
    let max_messages = matches
        .value_of("max_messages")
        .unwrap_or("100000")
        .parse()
        .expect("Number of messages must be a positive integer");

    let mut bus = MsgBus::new();
    let clients: Vec<Client> = (0..num_clients).map(|_| bus.add_client()).collect();
    let bus_thread = std::thread::spawn(|| {
        bus.run();
    });
    let client_threads: Vec<JoinHandle<()>> = clients
        .into_iter()
        .map(|client| {
            let max_messages = max_messages;
            std::thread::spawn(move || {
                client.run(max_messages);
            })
        })
        .collect();
    // Joining the spawned threads is not needed in this example, but it a good practice
    // and it propagates eventual panics during development.
    bus_thread.join().unwrap();
    client_threads
        .into_iter()
        .for_each(|thread| thread.join().unwrap());
}
