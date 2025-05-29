// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import './../dependencies/openzeppelin-contracts/contracts/utils/math/Math.sol';

struct Settings {
    uint16 maxOperatorDataBytes;
}

event MigrationStarted(uint64 id, MigrationPlan plan, uint128 clusterVersion);
event MigrationDataPullCompleted(uint64 id, address operatorAddress, uint128 clusterVersion);
event MigrationCompleted(uint64 id, address operatorAddress, uint128 clusterVersion);
event MigrationAborted(uint64 id, uint128 clusterVersion);

event MaintenanceStarted(address operatorAddress, uint128 clusterVersion);
event MaintenanceFinished(address operatorAddress, uint128 clusterVersion);

event NodeOperatorCreated(NodeOperator operator, uint128 clusterVersion);
event NodeOperatorUpdated(NodeOperator operator, uint128 clusterVersion);
event NodeOperatorDeleted(address operatorAddress, uint128 clusterVersion);

event SettingsUpdated(Settings newSettings, uint128 clusterVersion);

event OwnershipTransferred(address newOwner, uint128 cluserVersion);

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

    constructor(Settings memory initialSettings, NodeOperator[] memory initialOperators) {
        require(initialOperators.length <= 256, "too many operators");

        owner = msg.sender;
        settings = initialSettings;
                
        for (uint256 i = 0; i < initialOperators.length; i++) {
            validateOperatorDataSize(initialOperators[i].data.length);
            require(operatorData[initialOperators[i].addr].length == 0, "duplicate operator");

            keyspaces[0].operators[i] = initialOperators[i].addr;
            keyspaces[0].operatorIndexes[initialOperators[i].addr] = OptionU8({ is_some: true, value: uint8(i) });
            operatorData[initialOperators[i].addr] = initialOperators[i].data;
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
    
        Keyspace storage keyspace = keyspaces[keyspaceVersion % 2];
        keyspaceVersion++;
        Keyspace storage newKeyspace = keyspaces[keyspaceVersion % 2];

        // Make sure that initially the new keyspace is identical to the old one.
        cloneKeyspace(keyspace, newKeyspace);

        applyMigrationPlan(newKeyspace, plan);

        migration.pullingOperatorsBitmask = newKeyspace.operatorsBitmask;

        version++;
        emit MigrationStarted(migration.id, plan, version);
    }

    function cloneKeyspace(Keyspace storage src, Keyspace storage dst) internal {
        // Pick highest out of two to ensure that all non-empty slots are being processed.
        uint256 highestSlot = Math.max(src.operatorsBitmask.highest1(), dst.operatorsBitmask.highest1());
        address srcAddr;
        address dstAddr;
        for (uint256 i = 0; i <= highestSlot; i++) {
            srcAddr = src.operators[i];
            dstAddr = dst.operators[i];

            if (srcAddr == dstAddr) {
                continue;
            }

            if (dstAddr != address(0)) {
                delete(dst.operatorIndexes[dstAddr]);
            }

            if (srcAddr != address(0)) {
                dst.operatorIndexes[srcAddr] = OptionU8({ is_some: true, value: uint8(i) });
            }

            dst.operators[i] = srcAddr;
        }

        dst.replicationStrategy = src.replicationStrategy;
        dst.operatorsBitmask = src.operatorsBitmask;
    }

    function applyMigrationPlan(Keyspace storage keyspace, MigrationPlan calldata plan) internal {
        uint256 operatorsBitmask = keyspace.operatorsBitmask;

        uint8 idx;
        address addr;
        address newAddr;
        for (uint256 i = 0; i < plan.slots.length; i++) {
            idx = plan.slots[i].idx;
            newAddr = plan.slots[i].operatorAddress;
            addr = keyspace.operators[idx];

            require(addr != newAddr, "unchanged slot");

            if (addr != address(0)) {
                delete(keyspace.operatorIndexes[addr]);
            }

            if (newAddr == address(0)) {
                operatorsBitmask = operatorsBitmask.set0(idx);
            } else {
                require(operatorData[newAddr].length != 0, "unknown operator");
                require(!keyspace.operatorIndexes[newAddr].is_some, "operator duplicate");
                operatorsBitmask = operatorsBitmask.set1(idx);
            }

            keyspace.operators[idx] = newAddr;
            keyspace.operatorIndexes[newAddr] = OptionU8({ is_some: true, value: idx });
        }

        keyspace.operatorsBitmask = operatorsBitmask;
        keyspace.replicationStrategy = plan.replicationStrategy;
    }

    function completeMigration(uint64 id) external hasMigration {
        require(id == migration.id, "wrong migration id");

        OptionU8 memory operatorIdx = keyspaces[keyspaceVersion % 2].operatorIndexes[msg.sender];
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

    function abortMigration() external onlyOwner hasMigration {
        keyspaceVersion--;
        migration.pullingOperatorsBitmask = 0;

        version++;
        emit MigrationAborted(migration.id, version);
    }

    function startMaintenance() external noMigration noMaintenance {
        require(keyspaces[keyspaceVersion % 2].operatorIndexes[msg.sender].is_some || msg.sender == owner, "unauthorized");
        
        maintenance.slot = msg.sender;

        version++;
        emit MaintenanceStarted(msg.sender, version);
    }

    function finishMaintenance() external hasMaintenance {
        require(msg.sender == owner || maintenance.slot == msg.sender, "unauthorized");

        maintenance.slot = address(0);

        version++;
        emit MaintenanceFinished(msg.sender, version);
    }

    function createNodeOperator(NodeOperator calldata operator) external onlyOwner {
        require(operatorData[operator.addr].length == 0, "operator already exists");
        validateOperatorDataSize(operator.data.length);

        operatorData[operator.addr] = operator.data;
        version++;
        emit NodeOperatorCreated(operator, version);
    }

    function updateNodeOperator(NodeOperator calldata operator) external {
        require(msg.sender == owner || msg.sender == operator.addr, "unauthorized");
        require(operatorData[operator.addr].length != 0, "unknown operator");
        validateOperatorDataSize(operator.data.length);

        operatorData[operator.addr] = operator.data;
        version++;
        emit NodeOperatorUpdated(operator, version);
    }

    function deleteNodeOperator(address operatorAddress) external onlyOwner {
        require(operatorData[operatorAddress].length != 0, "unknown operator");

        if (isMigrationInProgress()) {
            require(!keyspaces[0].operatorIndexes[operatorAddress].is_some, "in keyspace");
            require(!keyspaces[1].operatorIndexes[operatorAddress].is_some, "in keyspace");
        } else {
            require(!keyspaces[keyspaceVersion % 2].operatorIndexes[operatorAddress].is_some, "in keyspace");
        }

        delete(operatorData[operatorAddress]);
        version++;
        emit NodeOperatorDeleted(operatorAddress, version);
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
    mapping(address => OptionU8) operatorIndexes;
    address[256] operators;
    uint256 operatorsBitmask;

    uint8 replicationStrategy;
}

struct OptionU8 {
    bool is_some;
    uint8 value;
}

struct OptionalUInt8 {
    bool exists;
    uint8 value;
}

struct KeyspaceView {
    NodeOperator[] operators;

    uint8 replicationStrategy;
}            

struct KeyspaceSlot {
    uint8 idx;
    address operatorAddress;
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
