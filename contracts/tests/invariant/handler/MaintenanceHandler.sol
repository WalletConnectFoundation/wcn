// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {BaseHandler} from "./BaseHandler.sol";
import {ClusterHarness} from "tests/unit/ClusterHarness.sol";
import {ClusterStore} from "tests/invariant/store/ClusterStore.sol";
import {Cluster} from "src/Cluster.sol";

/// @dev Handler for maintenance-related actions following the Lockup pattern
contract MaintenanceHandler is BaseHandler {
    /*//////////////////////////////////////////////////////////////////////////
                                   CONSTRUCTOR
    //////////////////////////////////////////////////////////////////////////*/
    
    constructor(ClusterHarness _cluster, ClusterStore _store, address _owner) 
        BaseHandler(_cluster, _store, _owner) {}
    
    /*//////////////////////////////////////////////////////////////////////////
                                HANDLER FUNCTIONS
    //////////////////////////////////////////////////////////////////////////*/
    
    /// @dev Start maintenance as owner
    function startMaintenanceAsOwner(
        uint256 timeJumpSeed
    ) 
        external 
        instrument("startMaintenanceAsOwner")
        adjustTimestamp(timeJumpSeed)
        useOwner 
    {
        // Assume maintenance is not already active
        vm.assume(cluster.maintenanceSlot() == address(0));
        
        // Assume no migration in progress
        vm.assume(cluster.getPullingOperators().length == 0);
        
        // Call function - fail_on_revert will catch bugs (using setMaintenance)
        cluster.setMaintenance(true);
    }
    
    /// @dev Start maintenance as operator
    function startMaintenanceAsOperator(
        uint256 timeJumpSeed,
        uint256 operatorSeed
    ) 
        external 
        instrument("startMaintenanceAsOperator")
        adjustTimestamp(timeJumpSeed)
        useFuzzedOperator(operatorSeed)
    {
        // Assume maintenance is not already active
        vm.assume(cluster.maintenanceSlot() == address(0));
        
        // Assume no migration in progress
        vm.assume(cluster.getPullingOperators().length == 0);
        
        // Call function as operator - fail_on_revert will catch bugs
        cluster.setMaintenance(true);
    }
    
    /// @dev End maintenance
    function endMaintenance(uint256 timeJumpSeed) 
        external 
        instrument("endMaintenance")
        adjustTimestamp(timeJumpSeed)
        useOwner 
    {
        // Assume maintenance is active
        vm.assume(cluster.maintenanceSlot() != address(0));
        
        // Call function - fail_on_revert will catch bugs
        cluster.setMaintenance(false);
    }
} 
