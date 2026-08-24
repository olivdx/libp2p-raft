//! Temp demo: build a sample Snapshot and print its contents + wire bytes.

use std::collections::BTreeMap;

use libp2p_raft::raft::types::{EntryType, LogEntry, Snapshot};
use libp2p_raft::storage::{memory::MemoryStorage, Storage};

fn hex_dump(bytes: &[u8]) -> String {
    bytes
        .chunks(16)
        .enumerate()
        .map(|(i, chunk)| {
            let hex: Vec<String> = chunk.iter().map(|b| format!("{b:02x}")).collect();
            let ascii: String = chunk
                .iter()
                .map(|&b| if (0x20..0x7f).contains(&b) { b as char } else { '.' })
                .collect();
            format!("{:04x}  {:<47}  {}", i * 16, hex.join(" "), ascii)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn print_sample_snapshot() {
    // 1. Application state machine: a KV store after applying entries 1..=5
    //    (set a=1, set b=2, set c=3, set b=20, del a)
    let mut kv: BTreeMap<String, u64> = BTreeMap::new();
    kv.insert("b".into(), 20);
    kv.insert("c".into(), 3);

    // The app serializes its FULL state — this becomes state_blob.
    let state_blob = bincode::serialize(&kv).unwrap();

    // 2. Log storage with entries 1..=7, then compact at index 5.
    let mut store = MemoryStorage::new();
    let entries: Vec<LogEntry> = (1..=7u64)
        .map(|i| LogEntry {
            index: i,
            term: 1,
            entry_type: EntryType::Command(format!("cmd-{i}").into_bytes()),
        })
        .collect();
    store.persist(None, &entries).unwrap();

    let snap = Snapshot {
        last_included_index: 5,
        last_included_term: 1,
        conf: vec![1, 2, 3],
        state_blob: state_blob.clone(),
    };
    store.install_snapshot(snap);

    let stored = store.snapshot().unwrap();
    println!("=== Snapshot (in-memory struct) ===");
    println!("last_included_index : {}", stored.last_included_index);
    println!("last_included_term  : {}", stored.last_included_term);
    println!("conf (voters)       : {:?}", stored.conf);
    println!("state_blob ({} bytes):", stored.state_blob.len());
    println!("{}", hex_dump(&stored.state_blob));

    let decoded: BTreeMap<String, u64> = bincode::deserialize(&stored.state_blob).unwrap();
    println!("\nstate_blob decoded back by the app: {decoded:?}");

    println!("\n=== Log after compaction ===");
    println!("first_index = {}", store.first_index());
    let (last, _) = store.last_index_term();
    for i in 1..=last {
        match store.entry(i) {
            Some(e) => println!("  entry {i}: term={} {:?}", e.term, e.entry_type),
            None => println!("  entry {i}: (compacted into snapshot)"),
        }
    }

    // 3. Wire form: the whole Snapshot as bincode (what InstallSnapshot chunks carry).
    let wire = bincode::serialize(&stored).unwrap();
    println!("\n=== Snapshot serialized (bincode, {} bytes) ===", wire.len());
    println!("{}", hex_dump(&wire));
}
