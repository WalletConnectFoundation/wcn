use std::{
    collections::{HashMap, HashSet},
    net::SocketAddrV4,
};

mod ed25519 {
    pub type PublicKey = [u8; 32];
}

type Address = [u8; 20];

// Just for demonstration purposes, should actually be a SlotMap.
type SlotMap<K, V> = HashMap<K, V>;

#[derive(Clone)]
struct Operator {
    ed25519_pub: ed25519::PublicKey,
    nodes: HashMap<SocketAddrV4, Node>,
}

#[derive(Clone)]
struct NewOperator {
    address: Address,
    inner: Operator,
}

#[derive(Clone)]
struct Node {
    // potentially more fields in the future
}

#[derive(Clone, Copy)]
struct Keyspace {
    version: u64,
}

struct Migration {
    remove: Vec<Address>,
    add: Vec<NewOperator>,

    /// Set of operators who still need to pull the data to finish the
    /// migration.
    pulling_operators: HashSet<Address>,

    new_keyspace: Keyspace,
}

struct Region {
    operators: SlotMap<Address, Operator>,

    under_maintenance: Option<Address>,

    keyspace: Keyspace,

    migration: Option<Migration>,
}

struct Contract {
    regions: Vec<Region>,

    version: u128,
}

impl Contract {
    const MAX_OPERATORS_PER_REGION: usize = 256;

    /// Begins a data migration process by adding/removing operators to/from a
    /// region.
    ///
    /// Requires "Admin" signature.
    fn start_migration(&mut self, region_id: u8, remove: Vec<Address>, add: Vec<NewOperator>) {
        let region = &mut self.regions[region_id as usize];

        assert!(region.migration.is_some(), "migration already in progress");
        assert!(!remove.is_empty() || !add.is_empty(), "nothing to do");

        for addr in &remove {
            assert!(region.operators.contains_key(addr), "unknown operator");
        }

        for new in &add {
            assert!(
                new.inner.nodes.len() >= 2,
                "operators should have at least 2 nodes"
            );

            assert!(
                !region.operators.contains_key(&new.address),
                "operator already exists"
            );
        }

        let mut pulling_operators: HashSet<_> = region
            .operators
            .keys()
            .chain(add.iter().map(|new| &new.address))
            .copied()
            .collect();

        for addr in &remove {
            pulling_operators.remove(addr);
        }

        region.migration = Some(Migration {
            remove: remove.clone(),
            add: add.clone(),
            pulling_operators,
            new_keyspace: Keyspace {
                version: region.keyspace.version + 1,
            },
        });

        self.version += 1;
        // emit_migration_started(region_id, remove, add, self.version);
    }

    /// Removes an operator from [`Migration::pending_operators`].
    ///
    /// Requires "Operator" signature.
    fn complete_migration(&mut self, region_id: u8, operator: Address) {
        let region = &mut self.regions[region_id as usize];

        let migration = region.migration.as_mut().expect("no migration");

        assert!(
            migration.pulling_operators.remove(&operator),
            "already completed"
        );

        self.version += 1;

        if !migration.pulling_operators.is_empty() {
            // emit_operator_migration_completed(region_id, operator, self.version);
            return;
        }

        region.keyspace = migration.new_keyspace;

        for addr in &migration.remove {
            region.operators.remove(addr);
        }

        // TODO: prefer replacing removed nodes.

        for new in &migration.add {
            region.operators.insert(new.address, new.inner.clone());
        }

        drop(migration);
        region.migration = None;

        // emit_migration_completed(region_id, self.version);
    }

    /// Aborts an ongoing data migration.
    ///
    /// Requires "Admin" signature.
    fn abort_migration(&mut self, region_id: u8) {
        let region = &mut self.regions[region_id as usize];

        assert!(region.migration.is_some(), "no migration");

        region.migration = None;
        self.version += 1;
        // emit_migration_aborted(region_id, self.version);
    }

    /// Acquires the maintenance slot for the provided operator.
    ///
    /// Requires "Operator" signature.
    fn start_maintenance(&mut self, region_id: u8, operator: Address) {
        let region = &mut self.regions[region_id as usize];

        assert!(region.migration.is_none(), "ongoing migration");
        assert!(region.under_maintenance.is_none(), "occupied");

        region.under_maintenance = Some(operator);
        self.version += 1;

        // emit_maintenance_started(region_id, operator, version);
    }

    /// Releases the maintenance slot occupied by the provided operator.
    ///
    /// Requires "Operator" signature.
    fn finish_maintenance(&mut self, region_id: u8, operator: Address) {
        let region = &mut self.regions[region_id as usize];

        assert_eq!(
            region.under_maintenance.as_ref(),
            Some(&operator),
            "not occupied by the provided operator"
        );

        region.under_maintenance = None;
        self.version += 1;

        // emit_maintenance_finished(region_id, operator, version);
    }

    /// Adds a (coordinator) node.
    ///
    /// Requires "Operator" signature.
    fn add_node(&mut self, region_id: u8, operator: Address, node_addr: SocketAddrV4) {
        let region = &mut self.regions[region_id as usize];
        let operator = region
            .operators
            .get_mut(&operator)
            .expect("unknown operator");

        assert!(!operator.nodes.contains_key(&node_addr), "already exists");
        operator.nodes.insert(node_addr, Node {});
        // emit_node_added(region_id, operator, node_addr, node);
    }

    /// Removes a (coordinator) node.
    ///
    /// Requires "Operator" signature.
    fn remove_node(&mut self, region_id: u8, operator: Address, node_addr: SocketAddrV4) {
        let region = &mut self.regions[region_id as usize];
        let operator = region
            .operators
            .get_mut(&operator)
            .expect("unknown operator");

        assert!(operator.nodes.contains_key(&node_addr), "not found");
        operator.nodes.remove(&node_addr);
        // emit_node_removed(region_id, operator, node_addr);
    }
}
