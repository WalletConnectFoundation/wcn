// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import '../dependencies/openzeppelin-contracts/utils/math/Math.sol';

struct Settings {
    uint16 maxOperatorDataBytes;
}

event MigrationStarted(uint64 id, Keyspace newKeyspace, uint64 newKeyspaceVersion, uint128 clusterVersion);
event MigrationDataPullCompleted(uint64 id, address operatorAddress, uint128 clusterVersion);
event MigrationCompleted(uint64 id, address operatorAddress, uint128 clusterVersion);
event MigrationAborted(uint64 id, uint128 clusterVersion);

event MaintenanceStarted(address addr, uint128 clusterVersion);
event MaintenanceFinished(uint128 clusterVersion);

event NodeOperatorAdded(uint8 idx, NodeOperator operator, uint128 clusterVersion);
event NodeOperatorUpdated(NodeOperator operator, uint128 clusterVersion);
event NodeOperatorRemoved(address operatorAddress, uint128 clusterVersion);

event SettingsUpdated(Settings newSettings, uint128 clusterVersion);

event OwnershipTransferred(address newOwner, uint128 cluserVersion);

contract Cluster {
    using Bitmask for uint256;

    // TODO: Should we just make all the fields public?

    address owner;

    NodeOperators nodeOperators;

    Keyspace[2] keyspaces;
    uint64 keyspaceVersion;

    Settings settings;
    Migration migration;
    Maintenance maintenance;

    uint128 version;

    constructor(Settings memory initialSettings, NodeOperator[] memory initialOperators) {
        require(initialOperators.length <= 256, "too many operators");

        owner = msg.sender;
        settings = initialSettings;
                
        for (uint256 i = 0; i < initialOperators.length; i++) {
            validateOperatorDataSize(initialOperators[i].data.length);
            require(!nodeOperators.indexes[initialOperators[i].addr].is_some, "duplicate operator");

            nodeOperators.indexes[initialOperators[i].addr] = OptionU8({ is_some: true, value: uint8(i) });
            nodeOperators.slots[i] = initialOperators[i];
        }

        nodeOperators.bitmask = Bitmask.fill(uint8(initialOperators.length));
        keyspaces[0].operatorsBitmask = nodeOperators.bitmask;
    }

    modifier onlyOwner() {
        require(msg.sender == owner, "not the owner");
        _;
    }

    modifier hasMigration() {
        require(isMigrationInProgress(), "no migration");
        _;
    }

    modifier noMigration() {
        require(!isMigrationInProgress(), "migration in progress");
        _;
    }

    modifier hasMaintenance() {
        require(isMaintenanceInProgress(), "no maintenance");
        _;
    }

    modifier noMaintenance() {
        require(!isMaintenanceInProgress(), "maintenance in progress");
        _;
    }

    function startMigration(Keyspace calldata newKeyspace) external onlyOwner noMigration noMaintenance {
        require(newKeyspace.operatorsBitmask.isSubsetOf(nodeOperators.bitmask), "invalid bitmask");

        Keyspace memory oldKeyspace = keyspaces[keyspaceVersion % 2];
        require(
            oldKeyspace.operatorsBitmask != newKeyspace.operatorsBitmask
            || oldKeyspace.replicationStrategy != newKeyspace.replicationStrategy,
            "same keyspace"
        );
    
        migration.id++;

        keyspaceVersion++;
        keyspaces[keyspaceVersion % 2] = newKeyspace;

        migration.pullingOperatorsBitmask = newKeyspace.operatorsBitmask;

        version++;
        emit MigrationStarted(migration.id, newKeyspace, keyspaceVersion, version);
    }

    function completeMigration(uint64 id) external hasMigration {
        require(id == migration.id, "wrong migration id");

        OptionU8 memory operatorIdx = nodeOperators.indexes[msg.sender];
        require(operatorIdx.is_some, "unknown operator");
        require(migration.pullingOperatorsBitmask.is1(operatorIdx.value), "not pulling");

        migration.pullingOperatorsBitmask = migration.pullingOperatorsBitmask.set0(operatorIdx.value);
    
        version++;
        if (migration.pullingOperatorsBitmask == 0) {
            emit MigrationCompleted(migration.id, msg.sender, version);
        } else {
            emit MigrationDataPullCompleted(migration.id, msg.sender, version);
        }
    }

    function abortMigration(uint64 id) external onlyOwner hasMigration {
        require(id == migration.id, "wrong migration id");

        keyspaceVersion--;
        migration.pullingOperatorsBitmask = 0;

        version++;
        emit MigrationAborted(migration.id, version);
    }

    function startMaintenance() external noMigration noMaintenance {
        require(nodeOperators.indexes[msg.sender].is_some || msg.sender == owner, "unauthorized");
        
        maintenance.slot = msg.sender;

        version++;
        emit MaintenanceStarted(msg.sender, version);
    }

    function finishMaintenance() external hasMaintenance {
        require(maintenance.slot == msg.sender || msg.sender == owner, "unauthorized");

        maintenance.slot = address(0);

        version++;
        emit MaintenanceFinished(version);
    }

    function addNodeOperator(uint8 idx, NodeOperator calldata operator) external onlyOwner {
        validateOperatorDataSize(operator.data.length);
        require(!nodeOperators.indexes[operator.addr].is_some, "already exists");
        require(nodeOperators.slots[idx].data.length == 0, "slot occupied");
        
        nodeOperators.indexes[operator.addr] = OptionU8({ is_some: true, value: idx });
        nodeOperators.slots[idx] = operator;
        nodeOperators.bitmask = nodeOperators.bitmask.set1(idx);

        version++;
        emit NodeOperatorAdded(idx, operator, version);
    }

    function updateNodeOperator(NodeOperator calldata operator) external {
        validateOperatorDataSize(operator.data.length);
        require(msg.sender == operator.addr || msg.sender == owner, "unauthorized");

        OptionU8 storage idx = nodeOperators.indexes[operator.addr];
        require(idx.is_some, "unknown operator");
        
        nodeOperators.slots[idx.value] = operator;
        version++;
        emit NodeOperatorUpdated(operator, version);
    }

    function removeNodeOperator(address operatorAddress) external onlyOwner {
        OptionU8 storage idx_opt = nodeOperators.indexes[operatorAddress];
        require(idx_opt.is_some, "unknown operator");
        uint8 idx = idx_opt.value;

        if (isMigrationInProgress()) {            
            require(keyspaces[0].operatorsBitmask.is0(idx) && keyspaces[1].operatorsBitmask.is0(idx), "in keyspace");
        } else {
            require(keyspaces[keyspaceVersion % 2].operatorsBitmask.is0(idx), "in keyspace");
        }
        
        delete(nodeOperators.indexes[operatorAddress]);
        delete(nodeOperators.slots[idx]);
        nodeOperators.bitmask = nodeOperators.bitmask.set0(idx);

        version++;
        emit NodeOperatorRemoved(operatorAddress, version);
    }

    function updateSettings(Settings calldata newSettings) external onlyOwner {
        settings = newSettings;
        version++;
        emit SettingsUpdated(newSettings, version);
    }

    function transferOwnership(address newOwner) external onlyOwner {
        owner = newOwner;
        version++;
        emit OwnershipTransferred(newOwner, version);
    }

    function getView() public view returns (ClusterView memory) {
        uint256 highestSlotIdx = nodeOperators.bitmask.highest1();
    
        ClusterView memory clusterView = ClusterView({
            owner: owner,
            nodeOperatorSlots: new NodeOperator[](highestSlotIdx + 1),
            keyspaces: keyspaces,
            keyspaceVersion: keyspaceVersion,
            settings: settings,
            migration: migration,
            maintenance: maintenance,
            version: version
        });

        uint256 nodeOperatorsBitmask = nodeOperators.bitmask;

        for (uint256 i = 0; i <= highestSlotIdx; i++) {
            if (nodeOperatorsBitmask.is0(uint8(i))) {
                continue;
            }

            clusterView.nodeOperatorSlots[i] = nodeOperators.slots[i];
        }
    
        return clusterView;
    }

    function validateOperatorDataSize(uint256 value) view internal {
        require(value > 0, "empty operator data");
        require(value <= settings.maxOperatorDataBytes, "operator data too large");
    }

    function isMigrationInProgress() view internal returns (bool) {
        return migration.pullingOperatorsBitmask != 0;
    }

    function isMaintenanceInProgress() view internal returns (bool) {
        return maintenance.slot != address(0);
    }
}

struct OptionU8 {
    bool is_some;
    uint8 value;
}

struct NodeOperator {
    address addr;
    bytes data;
}

struct NodeOperators {
    mapping(address => OptionU8) indexes;
    NodeOperator[256] slots;
    uint256 bitmask;
}

struct Keyspace {
    uint256 operatorsBitmask;
    uint8 replicationStrategy;
}

struct Migration {
    uint64 id;
    uint256 pullingOperatorsBitmask;
}

struct Maintenance {
    address slot;
}

struct ClusterView {
    address owner;

    NodeOperator[] nodeOperatorSlots;

    Keyspace[2] keyspaces;
    uint64 keyspaceVersion;

    Settings settings;
    Migration migration;
    Maintenance maintenance;

    uint128 version;
}

library Bitmask {
    function fill(uint8 n) internal pure returns (uint256) {
         return (1 << uint256(n)) - 1;
    }

    function set1(uint256 bitmask, uint8 n) internal pure returns (uint256) {
         return bitmask | 1 << uint256(n);
    }

    function set0(uint256 bitmask, uint8 n) internal pure returns (uint256) {
         return bitmask & ~(1 << uint256(n));
    }

    function is1(uint256 bitmask, uint8 n) internal pure returns (bool) {
         return (bitmask & (1 << uint256(n))) != 0;
    }

    function is0(uint256 bitmask, uint8 n) internal pure returns (bool) {
         return (bitmask & (1 << uint256(n))) == 0;
    }

    function count1(uint256 bitmask) internal pure returns (uint256 count) {
        while (bitmask != 0) {
            bitmask &= (bitmask - 1);
            count++;
        }
    }

    function highest1(uint256 bitmask) internal pure returns (uint256) {
        return Math.log2(bitmask);    
    }

    function isSubsetOf(uint256 self, uint256 other) internal pure returns (bool) {
        return self & ~other == 0;
    }
}
