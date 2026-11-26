use t3code_server::{http_known, rpc_known, HTTP_CONTRACTS, RPC_CONTRACTS};

#[test]
fn every_inventory_entry_is_queryable() {
    for contract in HTTP_CONTRACTS {
        assert!(
            http_known(contract.method, contract.path),
            "{} {}",
            contract.method,
            contract.path
        );
    }
    for contract in RPC_CONTRACTS {
        assert!(rpc_known(contract.tag), "{}", contract.tag);
    }
}

#[test]
fn inventory_counts_are_pinned_to_upstream_snapshot() {
    assert_eq!(HTTP_CONTRACTS.len(), 91);
    assert_eq!(RPC_CONTRACTS.len(), 261);
}
