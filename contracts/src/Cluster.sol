// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import '../dependencies/openzeppelin-contracts/contracts/utils/math/Math.sol';

struct Settings {
    uint16 maxOperatorDataBytes;
}

event MigrationStarted(uint64 id, MigrationPlan plan, uint128 clusterVersion);
event MigrationDataPullCompleted(uint64 id, address operatorAddress, uint128 clusterVersion);
event MigrationCompleted(uint64 id, address operatorAddress, uint128 clusterVersion);
event MigrationAborted(uint64 id, uint128 clusterVersion);

event MaintenanceStarted(address operatorAddress, uint128 clusterVersion);
event MaintenanceCompleted(address operatorAddress, uint128 clusterVersion);
event MaintenanceAborted(uint128 clusterVersion);

event NodeOperatorDataUpdated(address operatorAddress, bytes data, uint128 clusterVersion);

contract Cluster {
    using Bitmask for uint256;

    // TODO: Should we just make all the fields public?

    address owner;
    
    mapping(address => bytes) operatorData;

    Keyspace[2] keyspaces;
    uint64 keyspaceVersion;

    Settings settings;
    Migration migration;
    Maintenance maintenance;

    uint128 version;

    constructor(Settings memory initialSettings, address[] memory initialOperators) {
        owner = msg.sender;
        settings = initialSettings;
    
        for (uint256 i = 0; i < initialOperators.length; i++) {
            keyspaces[0].operators[i] = initialOperators[i]; 
        }
        keyspaces[0].operatorsBitmask = Bitmask.fill(uint8(initialOperators.length));
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

    function startMigration(MigrationPlan calldata plan) external onlyOwner noMigration noMaintenance {
        migration.id++;
    
        uint256 keyspaceIdx = keyspaceVersion % 2;
        keyspaceVersion++;
        uint256 migrationKeyspaceIdx = keyspaceVersion % 2;

        // NOTE: We are not zeroing out the rest of the buffer, so there might be some junk left.
        // The source of truth on whether the value is set or not should be the bitmask!
        for (uint256 i = 0; i <= keyspaces[keyspaceIdx].operatorsBitmask.highest1(); i++) {
            if (keyspaces[migrationKeyspaceIdx].operators[i] != keyspaces[keyspaceIdx].operators[i]) {
                keyspaces[migrationKeyspaceIdx].operators[i] = keyspaces[keyspaceIdx].operators[i];
            }
        }

        uint256 operatorsBitmask = keyspaces[keyspaceIdx].operatorsBitmask;

        uint8 idx;
        address addr; 
        for (uint256 i = 0; i < plan.slots.length; i++) {
            idx = plan.slots[i].idx;
            addr = plan.slots[i].operator;

            if (addr == address(0)) {
                operatorsBitmask = operatorsBitmask.set0(idx);
            } else {
                operatorsBitmask = operatorsBitmask.set1(idx);
            }

            keyspaces[migrationKeyspaceIdx].operators[idx] = addr;
        }

        keyspaces[migrationKeyspaceIdx].operatorsBitmask = operatorsBitmask;
        keyspaces[migrationKeyspaceIdx].replicationStrategy = plan.replicationStrategy;

        migration.pullingOperatorsBitmask = operatorsBitmask;

        version++;
        emit MigrationStarted(migration.id, plan, version);
    }

    function completeMigration(uint64 id, uint8 operatorIdx) external hasMigration {
        require(id == migration.id, "wrong migration id");
        require(keyspaces[keyspaceVersion % 2].operators[operatorIdx] == msg.sender, "wrong operator");
        if (migration.pullingOperatorsBitmask.is0(operatorIdx)) {
            return;
        }

        migration.pullingOperatorsBitmask = migration.pullingOperatorsBitmask.set0(operatorIdx);
    
        version++;
        if (migration.pullingOperatorsBitmask == 0) {
            emit MigrationCompleted(migration.id, msg.sender, version);
        } else {
            emit MigrationDataPullCompleted(migration.id, msg.sender, version);
        }
    }

    function abortMigration() external onlyOwner hasMigration {
        keyspaceVersion--;
        migration.pullingOperatorsBitmask = 0;

        version++;
        emit MigrationAborted(migration.id, version);
    }

    function startMaintenance(uint8 operatorIdx) external noMigration noMaintenance {
        require(keyspaces[keyspaceVersion % 2].operators[operatorIdx] == msg.sender, "wrong operator");
        
        maintenance.slot = msg.sender;

        version++;
        emit MaintenanceStarted(msg.sender, version);
    }

    function completeMaintenance() external hasMaintenance {
        require(maintenance.slot == msg.sender, "wrong operator");

        maintenance.slot = address(0);

        version++;
        emit MaintenanceCompleted(msg.sender, version);
    }

    function abortMaintenance() external onlyOwner hasMaintenance {
        maintenance.slot = address(0);
        
        version++;
        emit MaintenanceAborted(version);
    }

    function registerNodeOperator(bytes calldata data) external {
        validateOperatorDataSize(data.length);
        operatorData[msg.sender] = data;
    }

    function updateNodeOperatorData(uint8 operatorIdx, bytes calldata data) external {
        validateOperatorDataSize(data.length);

        bool inKeyspace;
        bool inMigrationKeyspace;

        if (isMigrationInProgress()) {
            inKeyspace = keyspaces[(keyspaceVersion - 1) % 2].operators[operatorIdx] == msg.sender;
            if (!inKeyspace) {
                inMigrationKeyspace = keyspaces[keyspaceVersion % 2].operators[operatorIdx] == msg.sender;
            }
        } else {
            inKeyspace = keyspaces[keyspaceVersion % 2].operators[operatorIdx] == msg.sender;
        }

        require(inKeyspace || inMigrationKeyspace, "wrong operator");

        operatorData[msg.sender] = data;
        version++;
        emit NodeOperatorDataUpdated(msg.sender, data, version);
    }

    function updateSettings(Settings calldata newSettings) external onlyOwner {
        settings = newSettings;
    }

    function transferOwnership(address newOwner) external onlyOwner {
        owner = newOwner;
    }

    function getView() public view returns (ClusterView memory) {
        ClusterView memory clusterView;

        uint256 keyspaceIdx;
        
        address addr;
        uint256 highestSlotIdx;
        
        if (isMigrationInProgress()) {
            clusterView.migration.id = migration.id;
            clusterView.migration.pullingOperatorsBitmask = migration.pullingOperatorsBitmask;

            keyspaceIdx = (keyspaceVersion - 1) % 2;
            uint256 migrationKeyspaceIdx = keyspaceVersion % 2;

            highestSlotIdx = keyspaces[migrationKeyspaceIdx].operatorsBitmask.highest1();
            clusterView.migrationKeyspace.operators = new NodeOperator[](highestSlotIdx + 1);
            clusterView.migrationKeyspace.replicationStrategy = keyspaces[migrationKeyspaceIdx].replicationStrategy;
            
            for (uint256 i = 0; i <= highestSlotIdx; i++) {
                if (keyspaces[migrationKeyspaceIdx].operatorsBitmask.is0(uint8(i))) {
                    continue;
                }
            
                addr = keyspaces[migrationKeyspaceIdx].operators[i];
                clusterView.migrationKeyspace.operators[i].addr = addr;

                // Populate data only if it won't be present in the primary keyspace view, to optimize the cluster view size.
                if (keyspaces[keyspaceIdx].operatorsBitmask.is0(uint8(i)) || keyspaces[keyspaceIdx].operators[i] != addr) {
                    clusterView.migrationKeyspace.operators[i].data = operatorData[addr];
                }
            }
        } else {
            keyspaceIdx = keyspaceVersion % 2;
        }

        highestSlotIdx = keyspaces[keyspaceIdx].operatorsBitmask.highest1();
        clusterView.keyspace.operators = new NodeOperator[](highestSlotIdx + 1); 
        clusterView.keyspace.replicationStrategy = keyspaces[keyspaceIdx].replicationStrategy;

        for (uint256 i = 0; i <= highestSlotIdx; i++) {
            if (keyspaces[keyspaceIdx].operatorsBitmask.is0(uint8(i))) {
                continue;
            }

            addr = keyspaces[keyspaceIdx].operators[i];
            clusterView.keyspace.operators[i].addr = addr;
            clusterView.keyspace.operators[i].data = operatorData[addr];
        }

        clusterView.keyspaceVersion = keyspaceVersion;
        clusterView.maintenance.slot = maintenance.slot;        
        clusterView.version = version;
    
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

struct NodeOperator {
    address addr;
    bytes data;
}

struct Keyspace {
    address[256] operators;
    uint256 operatorsBitmask;

    uint8 replicationStrategy;
}

struct KeyspaceView {
    NodeOperator[] operators;

    uint8 replicationStrategy;
}            

struct KeyspaceSlot {
    uint8 idx;
    address operator;
}

struct MigrationPlan {
    KeyspaceSlot[] slots;
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
    KeyspaceView keyspace;

    Migration migration;
    KeyspaceView migrationKeyspace;

    Maintenance maintenance;

    uint64 keyspaceVersion;
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
}
