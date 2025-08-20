// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {BaseHandler} from "./BaseHandler.sol";
import {ClusterHarness} from "tests/unit/ClusterHarness.sol";
import {ClusterStore} from "tests/invariant/store/ClusterStore.sol";
import {Cluster, NodeOperator} from "src/Cluster.sol";

/// @dev Handler for operator-related actions following the Lockup pattern
contract OperatorHandler is BaseHandler {
    /*//////////////////////////////////////////////////////////////////////////
                                     VARIABLES
    //////////////////////////////////////////////////////////////////////////*/
    
    // Constants
    uint16 constant MAX_DATA_BYTES = 4096;
    
    /*//////////////////////////////////////////////////////////////////////////
                                   CONSTRUCTOR
    //////////////////////////////////////////////////////////////////////////*/
    
    constructor(ClusterHarness _cluster, ClusterStore _store, address _owner) 
        BaseHandler(_cluster, _store, _owner) {}
    
    /*//////////////////////////////////////////////////////////////////////////
                                HANDLER FUNCTIONS
    //////////////////////////////////////////////////////////////////////////*/
    
    /// @dev Add a node operator (direct struct fuzzing like Lockup)
    function addNodeOperator(
        uint256 timeJumpSeed,
        NodeOperator memory operator
    ) 
        external 
        instrument("addNodeOperator")
        adjustTimestamp(timeJumpSeed)
        useOwner 
    {
        // We don't want to create more than a certain number of operators
        vm.assume(cluster.getOperatorCount() < MAX_OPERATOR_COUNT);
        
        // Bound the operator to valid values
        operator = _boundNodeOperator(operator);
        
        // Assume operator doesn't already exist
        vm.assume(!cluster.isOperator(operator.addr));
        
        // Add the operator - fail_on_revert will catch bugs
        cluster.addNodeOperator(operator);
        
        // Track in store
        store.updateOperatorAdded(operator.addr);
    }
    
    /// @dev Update an existing operator
    function updateNodeOperator(
        uint256 timeJumpSeed,
        uint256 operatorSeed,
        NodeOperator memory newOperatorData
    ) 
        external 
        instrument("updateNodeOperator")
        adjustTimestamp(timeJumpSeed)
        useFuzzedOperator(operatorSeed)
    {
        // Bound the new operator data
        newOperatorData = _boundNodeOperator(newOperatorData);
        
        // Keep the same address as the selected operator
        newOperatorData.addr = getCurrentCaller();
        
        // Update the operator - fail_on_revert will catch bugs
        cluster.updateNodeOperatorData(newOperatorData.addr, newOperatorData.data);
    }
    
    /// @dev Remove an operator (owner only)
    function removeNodeOperator(
        uint256 timeJumpSeed,
        uint256 operatorSeed
    ) 
        external 
        instrument("removeNodeOperator")
        adjustTimestamp(timeJumpSeed)
        useOwner 
    {
        address[] memory operators = cluster.getAllOperators();
        vm.assume(operators.length > 1); // Need more than minimum
        
        // Select operator to remove
        address operatorToRemove = operators[_bound(operatorSeed, 0, operators.length - 1)];
        
        // Assume operator is not in current keyspace (to avoid revert)
        vm.assume(!_isOperatorInCurrentKeyspace(operatorToRemove));
        
        // Remove the operator - fail_on_revert will catch bugs
        cluster.removeNodeOperator(operatorToRemove);
        
        // Track in store
        store.updateOperatorRemoved(operatorToRemove);
    }
    
    /*//////////////////////////////////////////////////////////////////////////
                                      HELPERS
    //////////////////////////////////////////////////////////////////////////*/
    
    /// @dev Bound NodeOperator struct to valid values (Lockup style)
    function _boundNodeOperator(NodeOperator memory operator) internal pure returns (NodeOperator memory) {
        // Bound the address to a reasonable range (avoiding zero address via vm.assume)
        operator.addr = address(uint160(_bound(uint160(operator.addr), 0x1000, 0x9999)));
        
        // Bound operator data
        if (operator.data.length == 0) {
            operator.data = "default_operator_data";
        } else if (operator.data.length > MAX_DATA_BYTES) {
            // Truncate to max size
            bytes memory boundedData = new bytes(MAX_DATA_BYTES);
            for (uint256 i = 0; i < MAX_DATA_BYTES; i++) {
                boundedData[i] = operator.data[i];
            }
            operator.data = boundedData;
        }
        
        // maintenance is already bool, no need to bound
        
        return operator;
    }
    
    /// @dev Check if an operator is in the current keyspace
    function _isOperatorInCurrentKeyspace(address operatorAddr) internal view returns (bool) {
        uint8 operatorSlot = cluster.operatorSlotIndexes(operatorAddr);
        
        // Get migration status to determine which keyspaces to check
        (, , bool migrationInProgress) = cluster.getMigrationStatus();
        
        if (migrationInProgress) {
            // During migration, check both keyspaces (like the actual removeNodeOperator does)
            return cluster.exposed_isSlotInKeyspace(operatorSlot, 0) || 
                   cluster.exposed_isSlotInKeyspace(operatorSlot, 1);
        } else {
            // No migration, check current keyspace only
            (uint8[] memory keyspaceMembers,) = cluster.getCurrentKeyspace();
            for (uint256 i = 0; i < keyspaceMembers.length; i++) {
                if (keyspaceMembers[i] == operatorSlot) {
                    return true;
                }
            }
            return false;
        }
    }
} 
