use std::io::BufRead;

use tokio::io::AsyncReadExt;

#[tokio::main]
async fn main() {
    // usage: cargo run --example simple root_dir
    let args = std::env::args().collect::<Vec<_>>();
    let root_dir = args.get(1).expect("root_dir not specified").to_string();
    let port = args.get(2).expect("port not specified");
    let addr = format!("/ip6/::/udp/{}/quic-v1", port).parse().unwrap();
    let mut disca = disca::Disca::new(root_dir, 10, 100, addr).await.unwrap();

    println!("addr: {}", disca.addr());
    println!("peer_id: {}", disca.peer_id());

    // read line from stdin
    // if line starts with "add", add file
    // if line starts with "get", get file
    // if line starts with "add_peer", add peer

    for line in std::io::BufReader::new(std::io::stdin()).lines() {
        let line = line.unwrap();
        if line.starts_with("add ") {
            // add <key>, <content>
            let key = line[4..].split(", ").next().unwrap();
            let content = line[4..].split(", ").nth(1).unwrap();
            disca.add(key, content.as_bytes()).await.unwrap();
        } else if line.starts_with("get ") {
            let path = line[4..].to_string();
            let file = disca.get(path).await.unwrap();
            if let Some(mut file) = file {
                let mut content = String::new();
                file.read_to_string(&mut content).await.unwrap();
                println!("{}", content);
            } else {
                println!("file not found");
            }
        } else if line.starts_with("add_peer ") {
            // add_peer <peer_addr>
            let addr = line[9..].to_string().parse().unwrap();
            disca.add_peer(addr).await.unwrap();
            println!("peer added")
        }
    }
}
