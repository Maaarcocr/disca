use std::collections::HashSet;

use crate::FileProvider;
use anyhow::Result;
use dashmap::DashMap;
use either::Either;
use futures::StreamExt;
use libp2p::{
    identify,
    kad::{self, QueryId, QueryResult},
    request_response::{self, Message, ProtocolSupport, RequestId},
    swarm::{NetworkBehaviour, SwarmEvent},
    Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use serde::{Deserialize, Serialize};
use tokio::{io, select};

#[derive(Debug, Clone)]
pub struct FileSharingP2P {
    command_sender: tokio::sync::mpsc::UnboundedSender<Command>,
    peer_id: PeerId,
    addr: Multiaddr,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct FileRequest {
    path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct FileResponse {
    content: Option<Vec<u8>>,
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    request_response: request_response::cbor::Behaviour<FileRequest, FileResponse>,
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    identify: identify::Behaviour,
}

impl FileSharingP2P {
    pub async fn new<T: FileProvider + Send + 'static + Sync>(
        addr: Multiaddr,
        file_provider: T,
    ) -> Result<Self> {
        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_quic()
            .with_behaviour(|key| Behaviour {
                kademlia: kad::Behaviour::new(
                    key.public().to_peer_id(),
                    kad::store::MemoryStore::new(key.public().to_peer_id()),
                ),
                request_response: request_response::cbor::Behaviour::new(
                    [(
                        StreamProtocol::new("/file-exchange/1"),
                        ProtocolSupport::Full,
                    )],
                    request_response::Config::default(),
                ),
                identify: identify::Behaviour::new(identify::Config::new(
                    "disca/v1".to_string(),
                    key.public(),
                )),
            })?
            .build();

        let peer_id = swarm.local_peer_id().clone();
        swarm
            .behaviour_mut()
            .kademlia
            .set_mode(Some(kad::Mode::Server));

        let (command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(async move {
            let mut event_loop = EventLoop {
                swarm,
                command_receiver,
                file_provider,
                pending_start_providing: Default::default(),
                pending_get_providers: Default::default(),
                pending_get_file: Default::default(),
                pending_start_listening: Default::default(),
            };
            event_loop.run().await;
        });

        let (addr_sender, addr_receiver) = tokio::sync::oneshot::channel();
        command_sender.send(Command::StartListening {
            addr: addr.clone(),
            sender: addr_sender,
        })?;
        let addr = addr_receiver.await??;

        Ok(FileSharingP2P {
            command_sender,
            peer_id,
            addr,
        })
    }

    pub async fn add_file(&self, path: String) -> Result<()> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.command_sender
            .send(Command::AddFile { path, sender })?;
        receiver.await?
    }

    pub async fn remove_file(&self, path: String) -> Result<()> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.command_sender
            .send(Command::RemoveFile { path, sender })?;
        receiver.await?
    }

    pub async fn get_file(&mut self, path: String) -> Result<Option<Vec<u8>>> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.command_sender
            .send(Command::GetFile { path, sender })?;
        receiver.await?
    }

    pub async fn add_peer(&mut self, addr: Multiaddr) -> Result<()> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.command_sender
            .send(Command::AddPeer { addr, sender })?;
        receiver.await?
    }

    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    pub fn addr(&self) -> &Multiaddr {
        &self.addr
    }
}

#[derive(Debug)]
enum Command {
    AddFile {
        path: String,
        sender: tokio::sync::oneshot::Sender<Result<()>>,
    },
    RemoveFile {
        path: String,
        sender: tokio::sync::oneshot::Sender<Result<()>>,
    },
    GetFile {
        path: String,
        sender: tokio::sync::oneshot::Sender<Result<Option<Vec<u8>>>>,
    },
    AddPeer {
        addr: Multiaddr,
        sender: tokio::sync::oneshot::Sender<Result<()>>,
    },
    StartListening {
        sender: tokio::sync::oneshot::Sender<Result<Multiaddr>>,
        addr: Multiaddr,
    },
}

struct EventLoop<T> {
    swarm: Swarm<Behaviour>,
    file_provider: T,
    command_receiver: tokio::sync::mpsc::UnboundedReceiver<Command>,
    pending_start_providing: DashMap<QueryId, tokio::sync::oneshot::Sender<Result<()>>>,
    pending_get_providers: DashMap<QueryId, tokio::sync::oneshot::Sender<Result<Option<Vec<u8>>>>>,
    pending_get_file: DashMap<RequestId, tokio::sync::oneshot::Sender<Result<Option<Vec<u8>>>>>,
    pending_start_listening: DashMap<
        libp2p::core::transport::ListenerId,
        tokio::sync::oneshot::Sender<Result<Multiaddr>>,
    >,
}

impl<T: FileProvider> EventLoop<T> {
    pub(crate) async fn run(&mut self) {
        loop {
            select! {
                command = self.command_receiver.recv() => {
                    self.handle_command(command);
                }
                event = self.swarm.next() => {
                    self.handle_event(event.expect("there should always be an event"));
                }
            }
        }
    }

    fn handle_command(&mut self, command: Option<Command>) {
        match command {
            Some(Command::AddFile { path, sender }) => self.add_file(path, sender),
            Some(Command::RemoveFile { path, sender }) => self.remove_file(path, sender),
            Some(Command::GetFile { path, sender }) => self.get_providers(path, sender),
            Some(Command::AddPeer { addr, sender }) => {
                if let Err(e) = self.swarm.dial(addr.clone()) {
                    sender.send(Err(e.into())).expect("send should work");
                } else {
                    sender.send(Ok(())).expect("send should work");
                }
            }
            Some(Command::StartListening { sender, addr }) => self.start_listening(addr, sender),
            None => {
                return;
            }
        }
    }

    fn start_listening(
        &mut self,
        addr: Multiaddr,
        sender: tokio::sync::oneshot::Sender<Result<Multiaddr>>,
    ) {
        match self.swarm.listen_on(addr) {
            Ok(listener_id) => {
                self.pending_start_listening.insert(listener_id, sender);
            }
            Err(e) => {
                sender.send(Err(e.into())).expect("send should work");
            }
        }
    }

    fn add_file(&mut self, path: String, sender: tokio::sync::oneshot::Sender<Result<()>>) {
        let query_id = self
            .swarm
            .behaviour_mut()
            .kademlia
            .start_providing(path.into_bytes().into());
        match query_id {
            Ok(query_id) => {
                self.pending_start_providing.insert(query_id, sender);
            }
            Err(e) => {
                sender.send(Err(e.into())).expect("send should work");
            }
        }
    }

    fn remove_file(&mut self, path: String, sender: tokio::sync::oneshot::Sender<Result<()>>) {
        self.swarm
            .behaviour_mut()
            .kademlia
            .stop_providing(&path.into_bytes().into());
        sender.send(Ok(())).expect("send should work");
    }

    fn get_providers(
        &mut self,
        path: String,
        sender: tokio::sync::oneshot::Sender<Result<Option<Vec<u8>>>>,
    ) {
        let query_id = self
            .swarm
            .behaviour_mut()
            .kademlia
            .get_providers(path.into_bytes().into());
        self.pending_get_providers.insert(query_id, sender);
    }

    fn get_file(
        &mut self,
        key: String,
        providers: HashSet<PeerId>,
        sender: tokio::sync::oneshot::Sender<Result<Option<Vec<u8>>>>,
    ) {
        if let Some(provider) = providers.iter().next() {
            let request_id = self
                .swarm
                .behaviour_mut()
                .request_response
                .send_request(provider, FileRequest { path: key });
            self.pending_get_file.insert(request_id, sender);
        } else {
            sender.send(Ok(None)).expect("send should work");
        }
    }

    fn handle_event(
        &mut self,
        event: SwarmEvent<BehaviourEvent, Either<Either<void::Void, io::Error>, io::Error>>,
    ) {
        match event {
            SwarmEvent::NewListenAddr {
                listener_id,
                address,
            } => {
                if let Some((_, sender)) = self.pending_start_listening.remove(&listener_id) {
                    sender.send(Ok(address)).expect("send should work");
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
                info,
                ..
            })) => {
                let peer_id = info.public_key.to_peer_id();
                let addr = info.listen_addrs.first().unwrap().clone();
                self.swarm
                    .behaviour_mut()
                    .kademlia
                    .add_address(&peer_id, addr.clone());
            }
            SwarmEvent::Behaviour(BehaviourEvent::Kademlia(
                kad::Event::OutboundQueryProgressed {
                    id,
                    result: QueryResult::StartProviding(result),
                    ..
                },
            )) => {
                if let Some((_, sender)) = self.pending_start_providing.remove(&id) {
                    sender
                        .send(result.map(|_| ()).map_err(|e| e.into()))
                        .expect("send should work");
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Kademlia(
                kad::Event::OutboundQueryProgressed {
                    id,
                    result: QueryResult::GetProviders(result),
                    ..
                },
            )) => {
                if let Some((_, sender)) = self.pending_get_providers.remove(&id) {
                    match result {
                        Ok(kad::GetProvidersOk::FoundProviders { key, providers }) => {
                            let key =
                                String::from_utf8(key.to_vec()).expect("key should be valid utf8");
                            self.get_file(key.into(), providers, sender);
                        }
                        Ok(kad::GetProvidersOk::FinishedWithNoAdditionalRecord {
                            closest_peers: _,
                        }) => {
                            sender.send(Ok(None)).expect("send should work");
                        }
                        Err(e) => {
                            sender.send(Err(e.into())).expect("send should work");
                        }
                    }
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(
                request_response::Event::Message { peer: _, message },
            )) => match message {
                Message::Request {
                    request_id: _,
                    request,
                    channel,
                } => {
                    self.handle_request(request, channel);
                }
                Message::Response {
                    request_id,
                    response,
                } => {
                    self.handle_response(request_id, response);
                }
            },
            _ => {}
        }
    }

    fn handle_request(
        &mut self,
        request: FileRequest,
        channel: request_response::ResponseChannel<FileResponse>,
    ) {
        let file_content = self.file_provider.get_file(request.path);
        self.swarm
            .behaviour_mut()
            .request_response
            .send_response(
                channel,
                FileResponse {
                    content: file_content,
                },
            )
            .expect("send should work");
    }

    fn handle_response(&mut self, request_id: RequestId, response: FileResponse) {
        if let Some((_, sender)) = self.pending_get_file.remove(&request_id) {
            sender.send(Ok(response.content)).expect("send should work");
        }
    }
}
