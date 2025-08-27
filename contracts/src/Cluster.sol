// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Ownable2StepUpgradeable} from "@openzeppelin/contracts-upgradeable/access/Ownable2StepUpgradeable.sol";
import {UUPSUpgradeable} from "@openzeppelin/contracts/proxy/utils/UUPSUpgradeable.sol";
import {LibSort} from "solady/utils/LibSort.sol";
import {LibBit} from "solady/utils/LibBit.sol";

struct Settings {
    uint16 maxOperatorDataBytes;
    uint8 minOperators;
    bytes extra;
}

struct NodeOperator {
    address addr;
    bytes data;
}

struct Keyspace {
    uint256 operatorBitmask;
    uint8 replicationStrategy;
}

struct Migration {
    uint64 id;
    uint64 startedAt;
    uint64 abortedAt;
    uint256 pullingOperatorBitmask;
}

struct ClusterView {
    address owner;
    Settings settings;
    uint128 version;
    uint64 keyspaceVersion;
    NodeOperator[] operatorSlots;
    Keyspace[2] keyspaces;
    Migration migration;
    address maintenanceSlot;
}

contract Cluster is Ownable2StepUpgradeable, UUPSUpgradeable {
    using LibSort for *;
    using Bitmask for uint256;

    /*//////////////////////////////////////////////////////////////////////////
                            CONSTANTS & ERRORS
    //////////////////////////////////////////////////////////////////////////*/

    // Maximum number of operators per cluster
    uint16 public constant MAX_OPERATORS = 256;

    error Unauthorized();
    error MigrationInProgress();
    error NoMigrationInProgress();
    error InvalidOperator();
    error OperatorNotFound();
    error OperatorNotPulling();
    error CallerNotOperator();
    error OperatorExists();
    error OperatorInKeyspace();
    error SameKeyspace();
    error WrongMigrationId();
    error InvalidOperatorData();
    error TooManyOperators();
    error InsufficientOperators();
    error MaintenanceInProgress();

    /*//////////////////////////////////////////////////////////////////////////
                                EVENTS
    //////////////////////////////////////////////////////////////////////////*/

    event ClusterInitialized(NodeOperator[] operators, Settings settings, uint128 version);
    event MigrationStarted(uint64 indexed id, Keyspace newKeyspace, uint64 keyspaceVersion, uint128 version);
    event MigrationDataPullCompleted(uint64 indexed id, address indexed operator, uint128 version);
    event MigrationCompleted(uint64 indexed id, address indexed operator, uint128 version);
    event MigrationAborted(uint64 indexed id, uint128 version);

    event NodeOperatorAdded(address indexed operator, uint8 indexed slot, bytes operatorData, uint128 version);
    event NodeOperatorUpdated(address indexed operator, uint8 indexed slot, bytes operatorData, uint128 version);
    event NodeOperatorRemoved(address indexed operator, uint8 indexed slot, uint128 version);

    event MaintenanceToggled(address indexed operator, bool active, uint128 version);
    event SettingsUpdated(Settings newSettings, address indexed updatedBy, uint128 version);

    /*//////////////////////////////////////////////////////////////////////////
                            STORAGE
    //////////////////////////////////////////////////////////////////////////*/

    Settings public settings;
    uint128 public version;
    
    // Slot-based operator storage (stable u8 indexing)
    NodeOperator[256] public operatorSlots;
    mapping(address => uint8) public operatorSlotIndexes;
    uint256 public operatorBitmask;
    
    // Keyspaces
    Keyspace[2] public keyspaces;
    uint64 public keyspaceVersion;
    
    // Migration
    Migration public migration;

    // Maintenance
    address public maintenanceSlot;

    // Storage gap for future variables (reserves space for rewards layer)
    uint256[45] private __gap;

    /*//////////////////////////////////////////////////////////////////////////
                            CONSTRUCTOR & INITIALIZERf
    //////////////////////////////////////////////////////////////////////////*/

    constructor() {
        _disableInitializers();
    }

    function initialize(Settings memory initialSettings, NodeOperator[] memory initialOperators) external initializer {
        if (initialOperators.length > MAX_OPERATORS) revert TooManyOperators();
        if (initialOperators.length < initialSettings.minOperators) revert InsufficientOperators();
        
        __Ownable2Step_init();
        __Ownable_init(msg.sender);
        
        settings = initialSettings;
        
        // Allocate slots for initial operators
        for (uint256 i = 0; i < initialOperators.length;) {
            NodeOperator memory op = initialOperators[i];
            _validateOperatorData(op.data);
            
            uint8 slotIdx = uint8(i); // Use sequential slots for initial operators
            if (operatorBitmask.isSet(slotIdx)) revert OperatorExists();
            
            operatorSlots[slotIdx] = op;
            operatorSlotIndexes[op.addr] = slotIdx;
            operatorBitmask = operatorBitmask.set(slotIdx);
            
            unchecked { ++i; }
        }
              
        keyspaces[0].operatorBitmask = Bitmask.fill(initialOperators.length);

        emit ClusterInitialized(initialOperators, settings, ++version);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            MODIFIERS
    //////////////////////////////////////////////////////////////////////////*/


    /*//////////////////////////////////////////////////////////////////////////
                            MIGRATION FUNCTIONS
    //////////////////////////////////////////////////////////////////////////*/

    function startMigration(Keyspace calldata newKeyspace) external onlyOwner {
        if (maintenanceSlot != address(0)) revert MaintenanceInProgress();
        if (migration.pullingOperatorBitmask > 0) revert MigrationInProgress();

        if (newKeyspace.operatorBitmask.countSet() < settings.minOperators) {
            revert InsufficientOperators();
        }

        if (!newKeyspace.operatorBitmask.isSubsetOf(operatorBitmask)) {
            revert OperatorNotFound();
        }
       
        // Check if different from current keyspace
        Keyspace storage keyspace = keyspaces[keyspaceVersion % 2];
        if (keyspace.operatorBitmask == newKeyspace.operatorBitmask && keyspace.replicationStrategy == newKeyspace.replicationStrategy) {
            revert SameKeyspace();
        }
        
        migration.id++;
        migration.startedAt = uint64(block.timestamp);
        migration.pullingOperatorBitmask = newKeyspace.operatorBitmask;

        // Set new keyspace
        keyspaceVersion++;
        keyspaces[keyspaceVersion % 2] = newKeyspace;
        
        emit MigrationStarted(migration.id, newKeyspace, keyspaceVersion, ++version);
    }

    function completeMigration(uint64 id) external {
        if (migration.pullingOperatorBitmask == 0) revert NoMigrationInProgress();
        if (id != migration.id) revert WrongMigrationId();
        
        uint8 slotIdx = operatorSlotIndexes[msg.sender];
        if (operatorSlots[slotIdx].addr != msg.sender) revert CallerNotOperator();
        if (migration.pullingOperatorBitmask.isUnset(slotIdx)) revert OperatorNotPulling();

        migration.pullingOperatorBitmask = migration.pullingOperatorBitmask.unset(slotIdx);
        
        if (migration.pullingOperatorBitmask == 0) {
            emit MigrationCompleted(id, msg.sender, ++version);
        } else {
            emit MigrationDataPullCompleted(id, msg.sender, ++version);
        }
    }

    function abortMigration(uint64 id) external onlyOwner {
        if (migration.pullingOperatorBitmask == 0) revert NoMigrationInProgress();
        if (id != migration.id) revert WrongMigrationId();

        migration.abortedAt = uint64(block.timestamp);
        migration.pullingOperatorBitmask = 0;
        
        // Revert keyspace
        keyspaceVersion--;
        
        emit MigrationAborted(id, ++version);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            OPERATOR MANAGEMENT
    //////////////////////////////////////////////////////////////////////////*/

    function addNodeOperator(NodeOperator calldata operator) external onlyOwner {
        if (operatorBitmask == type(uint256).max) revert TooManyOperators();
        if (operator.addr == address(0)) revert InvalidOperator();
        if (operatorSlotIndexes[operator.addr] != 0 || operatorSlots[0].addr == operator.addr) revert OperatorExists();
        
        _validateOperatorData(operator.data);
        
        uint8 slotIdx = operatorBitmask.findFirstUnset();
        
        operatorSlots[slotIdx] = operator;
        operatorSlotIndexes[operator.addr] = slotIdx;
        operatorBitmask = operatorBitmask.set(slotIdx);
        
        emit NodeOperatorAdded(operator.addr, slotIdx, operator.data, ++version);
    }

    function updateNodeOperatorData(address operatorAddress, bytes calldata newData) external {
        if (msg.sender != operatorAddress && msg.sender != owner()) revert Unauthorized();

        _validateOperatorData(newData);
        uint8 slotIdx = operatorSlotIndexes[operatorAddress];
        if (operatorSlots[slotIdx].addr != operatorAddress) revert OperatorNotFound();
        
        operatorSlots[slotIdx].data = newData;
        emit NodeOperatorUpdated(operatorAddress, slotIdx, newData, ++version);
    }

    function removeNodeOperator(address operatorAddr) external onlyOwner {
        uint8 slotIdx = operatorSlotIndexes[operatorAddr];
        if (operatorSlots[slotIdx].addr != operatorAddr) revert OperatorNotFound();
        if (operatorBitmask.countSet() <= settings.minOperators) revert InsufficientOperators();
        
        // Check not in active keyspaces
        if (migration.pullingOperatorBitmask > 0) {
            // During migration, check both keyspaces
            if (_isSlotInKeyspace(slotIdx, 0) || _isSlotInKeyspace(slotIdx, 1)) revert OperatorInKeyspace();
        } else {
            // No migration, check current keyspace
            if (_isSlotInKeyspace(slotIdx, uint8(keyspaceVersion % 2))) revert OperatorInKeyspace();
        }
        
        delete operatorSlots[slotIdx];
        delete operatorSlotIndexes[operatorAddr];
        operatorBitmask = operatorBitmask.unset(slotIdx);
        
        emit NodeOperatorRemoved(operatorAddr, slotIdx, ++version);
    }

    function setMaintenance(bool active) external {
        if (active) {
            // Starting maintenance - check mutex
            if (maintenanceSlot != address(0)) revert MaintenanceInProgress();
            maintenanceSlot = msg.sender;
        } else {
            // Ending maintenance - only the one who started can end it, OR the owner can always end it
            if (maintenanceSlot != msg.sender && msg.sender != owner()) revert Unauthorized();
            maintenanceSlot = address(0);
        }      

        if (msg.sender != owner()) {
            uint8 slotIdx = operatorSlotIndexes[msg.sender];
            if (operatorSlots[slotIdx].addr != msg.sender) revert Unauthorized();
        }
        
        emit MaintenanceToggled(msg.sender, active, ++version);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            SETTINGS
    //////////////////////////////////////////////////////////////////////////*/

    function updateSettings(Settings calldata newSettings) external onlyOwner {
        if (operatorBitmask.countSet() < newSettings.minOperators) revert InsufficientOperators();
        settings = newSettings;
        emit SettingsUpdated(newSettings, msg.sender, ++version);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            VIEW FUNCTIONS
    //////////////////////////////////////////////////////////////////////////*/

    function getOperatorCount() external view returns (uint16) {
        return operatorBitmask.countSet();
    }

    function getOperatorAt(uint8 slotIdx) external view returns (address) {
        if (operatorBitmask.isUnset(slotIdx)) revert OperatorNotFound();
        return operatorSlots[slotIdx].addr;
    }

    function isOperator(address addr) external view returns (bool) {
        uint8 slotIdx = operatorSlotIndexes[addr];
        return operatorSlots[slotIdx].addr == addr;
    }

    function getAllOperators() external view returns (address[] memory) {
        uint16 operatorCount = operatorBitmask.countSet();
    
        address[] memory operatorAddresses = new address[](operatorCount);
        uint256 count = 0;
        
        for (uint8 i = 0; i < MAX_OPERATORS && count < operatorCount;) {
            if (operatorBitmask.isSet(i)) {
                operatorAddresses[count] = operatorSlots[i].addr;
                unchecked { ++count; }
            }
            unchecked { ++i; }
        }
        
        operatorAddresses.sort();
        return operatorAddresses;
    }

    function getCurrentKeyspace() external view returns (uint8[] memory, uint8) {
        Keyspace storage currentKeyspace = keyspaces[keyspaceVersion % 2];
        return (currentKeyspace.operatorBitmask.intoArray(), currentKeyspace.replicationStrategy);
    }

    function getMigrationStatus() external view returns (uint64 id, uint16 remaining, bool inProgress) {
        return (migration.id, migration.pullingOperatorBitmask.countSet(), migration.pullingOperatorBitmask > 0);
    }

    function getPullingOperators() external view returns (uint8[] memory) {
        return migration.pullingOperatorBitmask.intoArray();
    }

    function getView() external view returns (ClusterView memory) {
        uint16 highestOccupiedSlot = operatorBitmask.findLastSet();

        NodeOperator[] memory operators = new NodeOperator[](highestOccupiedSlot + 1);

        for (uint16 i = 0; i <= highestOccupiedSlot; i++) {
            if (operatorBitmask.isSet(uint8(i))) {
                operators[i] = operatorSlots[uint8(i)];
            }
        }
        
        return ClusterView({
            owner: owner(),
            settings: settings,
            version: version,
            keyspaceVersion: keyspaceVersion,
            operatorSlots: operators,
            keyspaces: keyspaces,
            migration: migration,
            maintenanceSlot: maintenanceSlot
        });
    }

    /// @notice Required override for UUPS proxy implementation
    /// @dev Only the owner can upgrade the implementation
    function _authorizeUpgrade(address) internal override onlyOwner {}

    /*//////////////////////////////////////////////////////////////////////////
                            INTERNAL FUNCTIONS
    //////////////////////////////////////////////////////////////////////////*/

    function _validateOperatorData(bytes memory data) internal view {
        if (data.length == 0) revert InvalidOperatorData();
        if (data.length > settings.maxOperatorDataBytes) revert InvalidOperatorData();
    }

    function _isSlotInKeyspace(uint8 slot, uint8 keyspaceIndex) internal view returns (bool) {
        return keyspaces[keyspaceIndex].operatorBitmask.isSet(slot);
    }
}

library Bitmask {
    /// Creates a new bitmask with first `n` least significant bits set.
    function fill(uint256 n) internal pure returns (uint256) {
        return (n == 256) ? type(uint256).max : (1 << n) - 1;
    }

    /// Sets the bit under the specified index.
    function set(uint256 self, uint8 idx) internal pure returns (uint256) {
         return self | 1 << uint256(idx);
    }

    /// Unsets the bit under the specified index.
    function unset(uint256 self, uint8 idx) internal pure returns (uint256) {
         return self & ~(1 << uint256(idx));
    }

    /// Indicates whether the bit under the specified index is set.
    function isSet(uint256 self, uint8 idx) internal pure returns (bool) {
         return (self & (1 << uint256(idx))) != 0;
    }

    /// Indicates whether the bit under the specified index is unset.
    function isUnset(uint256 self, uint8 idx) internal pure returns (bool) {
         return (self & (1 << uint256(idx))) == 0;
    }

    /// Counts the number of set bits.
    function countSet(uint256 self) internal pure returns (uint16) {
        return uint16(LibBit.popCount(self));
    }

    /// Finds the index of the first unset bit.
    function findFirstUnset(uint256 self) internal pure returns (uint8) {
        return uint8(LibBit.ffs(~self));
    }

    /// Finds the index of the last set bit.
    function findLastSet(uint256 self) internal pure returns (uint8) {
        return uint8(LibBit.fls(self));
    }

    /// Indicates whether `self` is a subset of `other`.
    /// Meaning whether every set bit in `self` has a corresponding set bit in `other`.
    function isSubsetOf(uint256 self, uint256 other) internal pure returns (bool) {
        return self & ~other == 0;
    }

    /// Constructs bitmask from an array of indexes of set bits.
    function fromArray(uint8[] memory array) internal pure returns (uint256 bitmask) {
        for (uint256 i = 0; i < array.length;) {
            bitmask = Bitmask.set(bitmask, array[i]);
            unchecked { ++i; }
        }
    }

    /// Converts the bitmask into an array of indexes of set bits.
    function intoArray(uint256 self) internal pure returns (uint8[] memory) {
        uint8[] memory array = new uint8[](Bitmask.countSet(self));
        uint256 idx = 0;
        
        for (uint16 i = 0; i <= Bitmask.findLastSet(self); i++) {
            if (Bitmask.isSet(self, uint8(i))) {
                array[idx++] = uint8(i);
            }
        }

        return array;
    }
}
