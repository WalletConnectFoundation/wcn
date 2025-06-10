// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {EnumerableSet} from "@openzeppelin-contracts/utils/structs/EnumerableSet.sol";
import {Ownable2Step, Ownable} from "@openzeppelin-contracts/access/Ownable2Step.sol";
import {LibSort} from "solady/utils/LibSort.sol";

struct Settings {
    uint16 maxOperatorDataBytes;
}

struct NodeOperator {
    address addr;
    bytes data;
    bool maintenance;
}

struct Keyspace {
    address[] members;
    uint8 replicationStrategy;
}

contract Cluster is Ownable2Step {
    using EnumerableSet for EnumerableSet.AddressSet;
    using LibSort for *;

    /*//////////////////////////////////////////////////////////////////////////
                            CONSTANTS & ERRORS
    //////////////////////////////////////////////////////////////////////////*/

    // Maximum number of operators per cluster
    uint256 constant MAX_OPERATORS = 256;

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
    error NotInitialized();

    /*//////////////////////////////////////////////////////////////////////////
                                EVENTS
    //////////////////////////////////////////////////////////////////////////*/

    event ClusterInitialized(uint256 operatorCount, Settings settings, uint128 version);
    event MigrationStarted(uint64 indexed id, address[] operators, uint8 replicationStrategy, uint64 keyspaceVersion, uint128 version);
    event MigrationDataPullCompleted(uint64 indexed id, address indexed operator, uint128 version);
    event MigrationCompleted(uint64 indexed id, address indexed operator, uint128 version);
    event MigrationAborted(uint64 indexed id, uint128 version);

    event NodeOperatorAdded(address indexed operator, NodeOperator operatorData, uint128 version);
    event NodeOperatorUpdated(address indexed operator, NodeOperator operatorData, uint128 version);
    event NodeOperatorRemoved(address indexed operator, uint128 version);

    event MaintenanceToggled(address indexed operator, bool active, uint128 version);
    event SettingsUpdated(Settings newSettings, address indexed updatedBy, uint128 version);

    /*//////////////////////////////////////////////////////////////////////////
                            STORAGE
    //////////////////////////////////////////////////////////////////////////*/

    Settings public settings;
    uint128 public version;
    
    // Operators
    EnumerableSet.AddressSet private _operators;
    mapping(address => NodeOperator) public info;
    
    // Keyspaces
    Keyspace[2] public keyspaces;
    uint64 public keyspaceVersion;
    
    // Migration
    uint64 public migrationId;
    mapping(address => bool) public isPulling;
    uint16 public pullingCount;

    // Storage gap for future variables (reserves space for rewards layer)
    uint256[45] private __gap;

    /*//////////////////////////////////////////////////////////////////////////
                            CONSTRUCTOR
    //////////////////////////////////////////////////////////////////////////*/

    constructor(Settings memory initialSettings, NodeOperator[] memory initialOperators) Ownable(msg.sender) {
        if (initialOperators.length > MAX_OPERATORS) revert TooManyOperators();
        
        settings = initialSettings;
        
        for (uint256 i = 0; i < initialOperators.length;) {
            NodeOperator memory op = initialOperators[i];
            _validateOperatorData(op.data);
            
            if (!_operators.add(op.addr)) revert OperatorExists();
            info[op.addr] = op;
            
            unchecked { ++i; }
        }
        
        // Initialize first keyspace with all operators (sorted for efficiency)
        address[] memory sortedOperators = _operators.values();
        sortedOperators.sort();
        keyspaces[0].members = sortedOperators;

        emit ClusterInitialized(initialOperators.length, settings, version);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            MODIFIERS
    //////////////////////////////////////////////////////////////////////////*/

    modifier onlyOperatorOrOwner(address operatorAddr) {
        if (msg.sender != operatorAddr && msg.sender != owner()) revert Unauthorized();
        _;
    }

    modifier onlyInitialized() {
        if (_operators.length() == 0) revert NotInitialized();
        _;
    }

    /*//////////////////////////////////////////////////////////////////////////
                            MIGRATION FUNCTIONS
    //////////////////////////////////////////////////////////////////////////*/

    function startMigration(address[] calldata newOperators, uint8 replicationStrategy) external onlyOwner onlyInitialized {
        if (pullingCount > 0) revert MigrationInProgress();
        if (newOperators.length > MAX_OPERATORS) revert TooManyOperators();
        
        // Validate operators are sorted, unique, and exist (single loop for gas optimization)
        if (newOperators.length > 1 && !newOperators.isSortedAndUniquified()) revert InvalidOperator();
        for (uint256 i = 0; i < newOperators.length;) {
            address op = newOperators[i];
            if (!_operators.contains(op)) revert OperatorNotFound();
            isPulling[op] = true; // Set pulling in same loop
            unchecked { ++i; }
        }
        pullingCount = uint16(newOperators.length);
        
        // Check if different from current keyspace
        Keyspace memory current = keyspaces[keyspaceVersion % 2];
        if (current.replicationStrategy == replicationStrategy && 
            current.members.length == newOperators.length) {
            // Both sorted, compare directly
            bool same = true;
            for (uint256 i = 0; i < current.members.length;) {
                if (current.members[i] != newOperators[i]) {
                    same = false;
                    break;
                }
                unchecked { ++i; }
            }
            if (same) revert SameKeyspace();
        }
        
        migrationId++;
        keyspaceVersion++;
        
        // Set new keyspace
        Keyspace storage newKeyspace = keyspaces[keyspaceVersion % 2];
        newKeyspace.members = newOperators;
        newKeyspace.replicationStrategy = replicationStrategy;
        
        emit MigrationStarted(migrationId, newOperators, replicationStrategy, keyspaceVersion, ++version);
    }

    function completeMigration(uint64 id) external onlyInitialized {
        if (pullingCount == 0) revert NoMigrationInProgress();
        if (id != migrationId) revert WrongMigrationId();
        if (!_operators.contains(msg.sender)) revert CallerNotOperator();
        if (!isPulling[msg.sender]) revert OperatorNotPulling();
        
        isPulling[msg.sender] = false;
        unchecked { --pullingCount; }
        
        if (pullingCount == 0) {
            emit MigrationCompleted(migrationId, msg.sender, ++version);
        } else {
            emit MigrationDataPullCompleted(migrationId, msg.sender, ++version);
        }
    }

    function abortMigration(uint64 id) external onlyOwner {
        if (pullingCount == 0) revert NoMigrationInProgress();
        if (id != migrationId) revert WrongMigrationId();
        
        // Clear pulling state efficiently
        address[] memory currentOperators = keyspaces[keyspaceVersion % 2].members;
        for (uint256 i = 0; i < currentOperators.length;) {
            isPulling[currentOperators[i]] = false;
            unchecked { ++i; }
        }
        pullingCount = 0;
        
        // Revert keyspace
        keyspaceVersion--;
        
        emit MigrationAborted(migrationId, ++version);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            OPERATOR MANAGEMENT
    //////////////////////////////////////////////////////////////////////////*/

    function addNodeOperator(NodeOperator calldata operator) external onlyOwner {
        _validateOperatorData(operator.data);
        if (!_operators.add(operator.addr)) revert OperatorExists();
        
        info[operator.addr] = operator;
        emit NodeOperatorAdded(operator.addr, operator, ++version);
    }

    function updateNodeOperator(NodeOperator calldata operator) external onlyOperatorOrOwner(operator.addr) onlyInitialized {
        _validateOperatorData(operator.data);
        if (!_operators.contains(operator.addr)) revert OperatorNotFound();
        
        info[operator.addr] = operator;
        emit NodeOperatorUpdated(operator.addr, operator, ++version);
    }

    function removeNodeOperator(address operatorAddr) external onlyOwner {
        if (!_operators.remove(operatorAddr)) revert OperatorNotFound();
        
        // Check not in active keyspaces (keyspaces are sorted, use binary search)
        if (pullingCount > 0) {
            // During migration, check both keyspaces
            (bool found0,) = keyspaces[0].members.searchSorted(operatorAddr);
            (bool found1,) = keyspaces[1].members.searchSorted(operatorAddr);
            if (found0 || found1) revert OperatorInKeyspace();
        } else {
            // No migration, check current keyspace
            (bool found,) = keyspaces[keyspaceVersion % 2].members.searchSorted(operatorAddr);
            if (found) revert OperatorInKeyspace();
        }
        
        delete info[operatorAddr];
        emit NodeOperatorRemoved(operatorAddr, ++version);
    }

    function setMaintenance(bool active) external onlyInitialized {
        // Owner can set maintenance but shouldn't write to info mapping
        if (msg.sender == owner()) {
            emit MaintenanceToggled(msg.sender, active, ++version);
            return;
        }
        
        if (!_operators.contains(msg.sender)) revert Unauthorized();
        info[msg.sender].maintenance = active;
        emit MaintenanceToggled(msg.sender, active, ++version);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            SETTINGS
    //////////////////////////////////////////////////////////////////////////*/

    function updateSettings(Settings calldata newSettings) external onlyOwner {
        settings = newSettings;
        emit SettingsUpdated(newSettings, msg.sender, ++version);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            VIEW FUNCTIONS
    //////////////////////////////////////////////////////////////////////////*/

    function isInitialized() public view returns (bool) {
        return _operators.length() > 0;
    }

    function getOperatorCount() external view returns (uint256) {
        return _operators.length();
    }

    function getOperatorAt(uint256 index) external view returns (address) {
        return _operators.at(index);
    }

    function isOperator(address addr) external view returns (bool) {
        return _operators.contains(addr);
    }

    function getAllOperators() external view returns (address[] memory) {
        address[] memory operators = _operators.values();
        operators.sort();
        return operators;
    }

    function getCurrentKeyspace() external view returns (address[] memory, uint8) {
        Keyspace memory current = keyspaces[keyspaceVersion % 2];
        return (current.members, current.replicationStrategy);
    }

    function getMigrationStatus() external view returns (uint64 id, uint16 remaining, bool inProgress) {
        return (migrationId, pullingCount, pullingCount > 0);
    }

    function getPullingOperators() external view returns (address[] memory) {
        if (pullingCount == 0) return new address[](0);
        
        address[] memory pulling = new address[](pullingCount);
        address[] memory currentOperators = keyspaces[keyspaceVersion % 2].members;
        uint256 count = 0;
        
        for (uint256 i = 0; i < currentOperators.length;) {
            if (isPulling[currentOperators[i]]) {
                pulling[count++] = currentOperators[i];
            }
            unchecked { ++i; }
        }
        
        assembly {
            mstore(pulling, count)
        }
        
        return pulling;
    }

    function getMaintenanceOperators() external view returns (address[] memory) {
        address[] memory allOperators = _operators.values();
        allOperators.sort();
        address[] memory maintenance = new address[](allOperators.length);
        uint256 count = 0;
        
        for (uint256 i = 0; i < allOperators.length;) {
            if (info[allOperators[i]].maintenance) {
                maintenance[count] = allOperators[i];
                unchecked { ++count; }
            }
            unchecked { ++i; }
        }
        
        assembly {
            mstore(maintenance, count)
        }
        
        return maintenance;
    }

    /*//////////////////////////////////////////////////////////////////////////
                            INTERNAL FUNCTIONS
    //////////////////////////////////////////////////////////////////////////*/

    function _validateOperatorData(bytes memory data) internal view {
        if (data.length == 0) revert InvalidOperatorData();
        if (data.length > settings.maxOperatorDataBytes) revert InvalidOperatorData();
    }
}
