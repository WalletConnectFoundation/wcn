// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {BaseHandler} from "./BaseHandler.sol";
import {ClusterHarness} from "tests/unit/ClusterHarness.sol";
import {ClusterStore} from "tests/invariant/store/ClusterStore.sol";
import {Cluster, Bitmask} from "src/Cluster.sol";
import {keyspace} from "tests/Utils.sol";

/// @dev Handler for migration-related actions following the Lockup pattern
contract MigrationHandler is BaseHandler {
    using Bitmask for uint256;

    /*//////////////////////////////////////////////////////////////////////////
                                   CONSTRUCTOR
    //////////////////////////////////////////////////////////////////////////*/
    
    constructor(ClusterHarness _cluster, ClusterStore _store, address _owner) 
        BaseHandler(_cluster, _store, _owner) {}
    
    /*//////////////////////////////////////////////////////////////////////////
                                HANDLER FUNCTIONS
    //////////////////////////////////////////////////////////////////////////*/
    
    /// @dev Start migration (bound inputs, let fail_on_revert catch bugs)
    function startMigration(
        uint256 timeJumpSeed,
        uint8[] memory slots,
        uint8 strategy
    ) 
        external 
        instrument("startMigration")
        adjustTimestamp(timeJumpSeed)
        useOwner 
    {
        // Assume no migration in progress and no maintenance
        vm.assume(cluster.getPullingOperators().length == 0);
        vm.assume(cluster.maintenanceSlot() == address(0));
        
        // Bound strategy to valid range
        strategy = uint8(_bound(strategy, 0, 2));
        
        // Bound and validate slots
        slots = _boundValidSlots(slots);
        vm.assume(slots.length > 0);
        
        // Ensure it's different from current keyspace
        (uint8[] memory currentSlots, uint8 currentStrategy) = cluster.getCurrentKeyspace();
        vm.assume(!_sameKeyspace(slots, strategy, currentSlots, currentStrategy));
        
        // Call function - fail_on_revert will catch bugs
        cluster.startMigration(keyspace(slots, strategy));
        
        // Track migration in store
        address[] memory migrationOps = new address[](slots.length);
        for (uint256 i = 0; i < slots.length; i++) {
            (address addr, ) = cluster.operatorSlots(slots[i]);
            migrationOps[i] = addr;
        }
        (uint64 migrationId,,) = cluster.getMigrationStatus();
        store.pushMigration(migrationId, migrationOps);
    }
    
    /// @dev Complete migration as a pulling operator
    function completeMigration(
        uint256 timeJumpSeed,
        uint256 actorSeed
    ) 
        external 
        instrument("completeMigration")
        adjustTimestamp(timeJumpSeed)
    {
        uint8[] memory pullingOps = cluster.getPullingOperators();
        vm.assume(pullingOps.length > 0);
        
        // Select a pulling operator to act as
        uint8 slot = pullingOps[_bound(actorSeed, 0, pullingOps.length - 1)];
        (address addr, ) = cluster.operatorSlots(slot);
        address pullingOperator = addr;
        
        (uint64 migrationId,,) = cluster.getMigrationStatus();
        
        // Call function as pulling operator - fail_on_revert will catch bugs
        resetPrank(pullingOperator);
        cluster.completeMigration(migrationId);
    }
    
    /// @dev Abort ongoing migration
    function abortMigration(uint256 timeJumpSeed) 
        external 
        instrument("abortMigration")
        adjustTimestamp(timeJumpSeed)
        useOwner 
    {
        // Assume migration is in progress
        (uint64 migrationId,, bool inProgress) = cluster.getMigrationStatus();
        vm.assume(inProgress);
        
        // Call function - fail_on_revert will catch bugs
        cluster.abortMigration(migrationId);
    }
    
    /*//////////////////////////////////////////////////////////////////////////
                                     HELPERS
    //////////////////////////////////////////////////////////////////////////*/
    
    /// @dev Bound slots array to valid, sorted, unique values
    function _boundValidSlots(uint8[] memory slots) internal view returns (uint8[] memory) {
        address[] memory operators = cluster.getAllOperators();
        if (operators.length == 0) return new uint8[](0);
        
        // Limit to reasonable size
        uint256 maxSlots = operators.length < 10 ? operators.length : 10;
        if (slots.length > maxSlots) {
            uint8[] memory bounded = new uint8[](maxSlots);
            for (uint256 i = 0; i < maxSlots; i++) {
                bounded[i] = slots[i];
            }
            slots = bounded;
        }
        
        if (slots.length == 0) {
            // Return a single valid slot
            uint8[] memory singleSlot = new uint8[](1);
            singleSlot[0] = cluster.operatorSlotIndexes(operators[0]);
            return singleSlot;
        }
        
        // Bound each slot to valid operator slots and remove duplicates
        for (uint256 i = 0; i < slots.length; i++) {
            slots[i] = uint8(_bound(slots[i], 0, 254));
            // Make sure slot is occupied
            if (cluster.operatorBitmask().isUnset(slots[i])) {
                slots[i] = cluster.operatorSlotIndexes(operators[i % operators.length]);
            }
        }
        
        // Sort and dedupe
        return _sortAndDedupe(slots);
    }
    
    /// @dev Check if two keyspaces are the same
    function _sameKeyspace(
        uint8[] memory slots1, uint8 strategy1,
        uint8[] memory slots2, uint8 strategy2
    ) internal pure returns (bool) {
        if (strategy1 != strategy2 || slots1.length != slots2.length) {
            return false;
        }
        
        for (uint256 i = 0; i < slots1.length; i++) {
            if (slots1[i] != slots2[i]) {
                return false;
            }
        }
        
        return true;
    }
    
    /// @dev Sort and remove duplicates from slots array
    function _sortAndDedupe(uint8[] memory slots) internal pure returns (uint8[] memory) {
        if (slots.length <= 1) return slots;
        
        // Simple bubble sort for small arrays
        for (uint256 i = 0; i < slots.length - 1; i++) {
            for (uint256 j = 0; j < slots.length - i - 1; j++) {
                if (slots[j] > slots[j + 1]) {
                    (slots[j], slots[j + 1]) = (slots[j + 1], slots[j]);
                }
            }
        }
        
        // Remove duplicates
        uint256 uniqueCount = 1;
        for (uint256 i = 1; i < slots.length; i++) {
            if (slots[i] != slots[i - 1]) {
                slots[uniqueCount] = slots[i];
                uniqueCount++;
            }
        }
        
        // Create array with unique elements
        uint8[] memory unique = new uint8[](uniqueCount);
        for (uint256 i = 0; i < uniqueCount; i++) {
            unique[i] = slots[i];
        }
        
        return unique;
    }
} 
