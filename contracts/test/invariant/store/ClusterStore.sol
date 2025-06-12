// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {ClusterView} from "../../../src/Cluster.sol";

/// @dev Storage variables needed by all cluster handlers
/// Follows the Lockup store pattern - simple storage, no events, no complex logic
contract ClusterStore {
    /*//////////////////////////////////////////////////////////////////////////
                                     VARIABLES
    //////////////////////////////////////////////////////////////////////////*/

    // State tracking
    mapping(uint128 version => bool recorded) public isPreviousViewRecorded;
    mapping(uint128 version => ClusterView clusterView) private _previousViewOf;
    uint128 public lastVersion;
    
    // Migration tracking
    mapping(uint64 migrationId => bool seen) public migrationSeen;
    mapping(uint64 migrationId => address[] operators) public migrationOperators;
    uint64[] public migrationIds;
    
    // Operator tracking
    mapping(address operator => bool added) public operatorEverAdded;
    mapping(address operator => bool removed) public operatorEverRemoved;
    address[] public allSeenOperators;
    
    /*//////////////////////////////////////////////////////////////////////////
                                      HELPERS
    //////////////////////////////////////////////////////////////////////////*/

    function pushMigration(uint64 migrationId, address[] memory operators) external {
        if (!migrationSeen[migrationId]) {
            migrationIds.push(migrationId);
            migrationSeen[migrationId] = true;
            migrationOperators[migrationId] = operators;
        }
    }

    function updateIsPreviousViewRecorded(uint128 version) external {
        isPreviousViewRecorded[version] = true;
    }

    function updatePreviousViewOf(uint128 version, ClusterView memory clusterView) external {
        _previousViewOf[version] = clusterView;
        lastVersion = version;
    }

    function updateOperatorAdded(address operator) external {
        if (!operatorEverAdded[operator]) {
            operatorEverAdded[operator] = true;
            allSeenOperators.push(operator);
        }
    }

    function updateOperatorRemoved(address operator) external {
        operatorEverRemoved[operator] = true;
    }
    
    /// @dev Get the length of migrationIds array
    function getMigrationCount() external view returns (uint256) {
        return migrationIds.length;
    }
    
    /// @dev Get the length of allSeenOperators array  
    function getOperatorCount() external view returns (uint256) {
        return allSeenOperators.length;
    }

    function previousViewOf(uint128 version) external view returns (ClusterView memory) {
        return _previousViewOf[version];
    }
} 