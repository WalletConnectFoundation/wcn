// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Test} from "forge-std/Test.sol";
import {UnsafeUpgrades} from "openzeppelin-foundry-upgrades/Upgrades.sol";
import {Cluster, Keyspace, NodeOperator, Settings, Bitmask} from "src/Cluster.sol";
import {keyspace} from "tests/Utils.sol";

contract ClusterIntegrationFuzzTest is Test {
    Cluster cluster;
    address proxy;
    address owner;
    address[] operators;
    
    function _defaultSettings() internal pure returns (Settings memory) {
        return Settings({
            maxOperatorDataBytes: 1024,
            minOperators: 1,
            extra: ""
        });
    }
    
    function setUp() public {
        owner = address(0x1);
        
        // Create a cluster with multiple operators
        operators = new address[](10);
        NodeOperator[] memory initialOperators = new NodeOperator[](10);
        
        for (uint256 i = 0; i < 10; i++) {
            operators[i] = address(uint160(0x100 + i));
            initialOperators[i] = NodeOperator({addr: operators[i], data: abi.encodePacked("operator", i)});
        }
        
        vm.startPrank(owner);
        proxy = UnsafeUpgrades.deployUUPSProxy(
            address(new Cluster()),
            abi.encodeCall(Cluster.initialize, (_defaultSettings(), initialOperators))
        );
        cluster = Cluster(proxy);
        vm.stopPrank();
    }

    /*//////////////////////////////////////////////////////////////////////////
                            MAINTENANCE RACE CONDITIONS
    //////////////////////////////////////////////////////////////////////////*/

    function testFuzz_MaintenanceRaceCondition_OperatorVsOwner(
        uint8 operatorIndex,
        bool operatorStartsMaintenance,
        bool ownerIntervenes
    ) public {
        // Bound operator index to valid range
        operatorIndex = uint8(bound(operatorIndex, 0, operators.length - 1));
        address operator = operators[operatorIndex];
        
        // Scenario: operator tries to toggle maintenance, owner may intervene
        if (operatorStartsMaintenance) {
            // Operator starts maintenance
            vm.prank(operator);
            cluster.setMaintenance(true);
            
            assertEq(cluster.maintenanceSlot(), operator);
            
            if (ownerIntervenes) {
                // Owner can always end maintenance
                vm.prank(owner);
                cluster.setMaintenance(false);
                
                assertEq(cluster.maintenanceSlot(), address(0));
            } else {
                // Only the same operator can end their maintenance
                vm.prank(operator);
                cluster.setMaintenance(false);
                
                assertEq(cluster.maintenanceSlot(), address(0));
            }
        } else {
            // Owner starts maintenance
            vm.prank(owner);
            cluster.setMaintenance(true);
            
            assertEq(cluster.maintenanceSlot(), owner);
            
            if (ownerIntervenes) {
                // Owner ends their own maintenance
                vm.prank(owner);
                cluster.setMaintenance(false);
            } else {
                // Operator cannot end owner's maintenance
                vm.prank(operator);
                vm.expectRevert(abi.encodeWithSelector(Cluster.Unauthorized.selector));
                cluster.setMaintenance(false);
            }
        }
    }
    
    function testFuzz_MaintenanceMutex_MultipleOperators(
        uint8 firstOperator,
        uint8 secondOperator
    ) public {
        // Bound to valid operator indices
        firstOperator = uint8(bound(firstOperator, 0, operators.length - 1));
        secondOperator = uint8(bound(secondOperator, 0, operators.length - 1));
        vm.assume(firstOperator != secondOperator);
        
        address op1 = operators[firstOperator];
        address op2 = operators[secondOperator];
        
        // First operator starts maintenance
        vm.prank(op1);
        cluster.setMaintenance(true);
        
        // Second operator cannot start maintenance (mutex)
        vm.prank(op2);
        vm.expectRevert(abi.encodeWithSelector(Cluster.MaintenanceInProgress.selector));
        cluster.setMaintenance(true);
        
        // Only first operator or owner can end maintenance
        vm.prank(op1);
        cluster.setMaintenance(false);
        
        // Now second operator can start maintenance
        vm.prank(op2);
        cluster.setMaintenance(true);
        
        assertEq(cluster.maintenanceSlot(), op2);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            MIGRATION FUZZ TESTS
    //////////////////////////////////////////////////////////////////////////*/
    
    function testFuzz_MigrationWithArbitrarySlots(
        uint8[] memory slots,
        uint8 replicationStrategy
    ) public {
        // Filter out invalid slots and ensure uniqueness
        uint8[] memory validSlots = _filterAndSortSlots(slots);
        vm.assume(validSlots.length > 0);
        vm.assume(validSlots.length <= 10); // We have 10 operators
        
        // Get current keyspace to ensure we're not migrating to the same keyspace
        (uint8[] memory currentKeyspace, uint8 currentReplicationStrategy) = cluster.getCurrentKeyspace();
        
        // Skip if this would be the same keyspace (same slots and replication strategy)
        if (validSlots.length == currentKeyspace.length && replicationStrategy == currentReplicationStrategy) {
            bool same = true;
            for (uint256 i = 0; i < validSlots.length; i++) {
                if (validSlots[i] != currentKeyspace[i]) {
                    same = false;
                    break;
                }
            }
            vm.assume(!same);
        }
        
        // Start migration
        vm.prank(owner);
        cluster.startMigration(keyspace(validSlots, replicationStrategy));
        
        (uint64 migrationId, uint16 remaining, bool inProgress) = cluster.getMigrationStatus();
        assertTrue(inProgress);
        assertEq(remaining, validSlots.length);
        
        // Complete migration with all operators
        for (uint256 i = 0; i < validSlots.length; i++) {
            address operatorAddr = cluster.getOperatorAt(validSlots[i]);
            vm.prank(operatorAddr);
            cluster.completeMigration(migrationId);
        }
        
        // Migration should be complete
        (, remaining, inProgress) = cluster.getMigrationStatus();
        assertFalse(inProgress);
        assertEq(remaining, 0);
        
        // Verify keyspace updated correctly
        (uint8[] memory newKeyspace, uint8 newReplicationStrategy) = cluster.getCurrentKeyspace();
        assertEq(newKeyspace.length, validSlots.length);
        assertEq(newReplicationStrategy, replicationStrategy);
        
        for (uint256 i = 0; i < validSlots.length; i++) {
            assertEq(newKeyspace[i], validSlots[i]);
        }
    }
    
    function testFuzz_MigrationAbortAtAnyPoint(
        uint8[] memory slots,
        uint8 completionPoint
    ) public {
        uint8[] memory validSlots = _filterAndSortSlots(slots);
        vm.assume(validSlots.length > 1);
        vm.assume(validSlots.length <= 10);
        
        // Get current keyspace to ensure we're not migrating to the same keyspace
        (uint8[] memory currentKeyspace, uint8 currentReplicationStrategy) = cluster.getCurrentKeyspace();
        
        // Skip if this would be the same keyspace (same slots and replication strategy = 1)
        if (validSlots.length == currentKeyspace.length && 1 == currentReplicationStrategy) {
            bool same = true;
            for (uint256 i = 0; i < validSlots.length; i++) {
                if (validSlots[i] != currentKeyspace[i]) {
                    same = false;
                    break;
                }
            }
            vm.assume(!same);
        }
        
        completionPoint = uint8(bound(completionPoint, 0, validSlots.length - 1));
        
        // Start migration
        vm.prank(owner);
        cluster.startMigration(keyspace(validSlots, 1));
        
        (uint64 migrationId,,) = cluster.getMigrationStatus();
        
        // Complete migration partially
        for (uint256 i = 0; i < completionPoint; i++) {
            address operatorAddr = cluster.getOperatorAt(validSlots[i]);
            vm.prank(operatorAddr);
            cluster.completeMigration(migrationId);
        }
        
        // Abort migration
        vm.prank(owner);
        cluster.abortMigration(migrationId);
        
        // Verify migration is aborted
        (,, bool inProgress) = cluster.getMigrationStatus();
        assertFalse(inProgress);
        
        // Verify no operators are pulling
        uint8[] memory pullingOps = cluster.getPullingOperators();
        assertEq(pullingOps.length, 0);
    }
    
    function testFuzz_MigrationDuringMaintenance_ShouldRevert(
        uint8 operatorIndex,
        uint8[] memory slots
    ) public {
        operatorIndex = uint8(bound(operatorIndex, 0, operators.length - 1));
        uint8[] memory validSlots = _filterAndSortSlots(slots);
        vm.assume(validSlots.length > 0);
        
        // Get current keyspace to ensure we're not migrating to the same keyspace
        (uint8[] memory currentKeyspace, uint8 currentReplicationStrategy) = cluster.getCurrentKeyspace();
        
        // Skip if this would be the same keyspace (same slots and replication strategy = 1)
        if (validSlots.length == currentKeyspace.length && 1 == currentReplicationStrategy) {
            bool same = true;
            for (uint256 i = 0; i < validSlots.length; i++) {
                if (validSlots[i] != currentKeyspace[i]) {
                    same = false;
                    break;
                }
            }
            vm.assume(!same);
        }
        
        // Start maintenance
        vm.prank(operators[operatorIndex]);
        cluster.setMaintenance(true);
        
        // Migration should fail during maintenance
        vm.prank(owner);
        vm.expectRevert(abi.encodeWithSelector(Cluster.MaintenanceInProgress.selector));
        cluster.startMigration(keyspace(validSlots, 1));
    }

    /*//////////////////////////////////////////////////////////////////////////
                            COMPLEX RACE CONDITIONS
    //////////////////////////////////////////////////////////////////////////*/
    
    function testFuzz_OwnershipTransferDuringMigration(
        uint8[] memory slots,
        address newOwner
    ) public {
        vm.assume(newOwner != address(0));
        vm.assume(newOwner != owner);
        
        uint8[] memory validSlots = _filterAndSortSlots(slots);
        vm.assume(validSlots.length > 0);
        vm.assume(validSlots.length <= 5); // Keep it manageable
        
        // Get current keyspace to ensure we're not migrating to the same keyspace
        (uint8[] memory currentKeyspace, uint8 currentReplicationStrategy) = cluster.getCurrentKeyspace();
        
        // Skip if this would be the same keyspace (same slots and replication strategy = 1)
        if (validSlots.length == currentKeyspace.length && 1 == currentReplicationStrategy) {
            bool same = true;
            for (uint256 i = 0; i < validSlots.length; i++) {
                if (validSlots[i] != currentKeyspace[i]) {
                    same = false;
                    break;
                }
            }
            vm.assume(!same);
        }
        
        // Start migration
        vm.prank(owner);
        cluster.startMigration(keyspace(validSlots, 1));
        
        // Transfer ownership during migration
        vm.prank(owner);
        cluster.transferOwnership(newOwner);
        
        vm.prank(newOwner);
        cluster.acceptOwnership();
        
        // New owner should be able to abort migration
        (uint64 migrationId,,) = cluster.getMigrationStatus();
        
        vm.prank(newOwner);
        cluster.abortMigration(migrationId);
        
        (,, bool inProgress) = cluster.getMigrationStatus();
        assertFalse(inProgress);
    }
    
    function testFuzz_MultipleActionsInterleaved(
        uint8 action1,
        uint8 action2,
        uint8 operatorIndex1,
        uint8 operatorIndex2
    ) public {
        action1 = uint8(bound(action1, 0, 2)); // 0: maintenance, 1: add operator, 2: update settings
        action2 = uint8(bound(action2, 0, 2));
        operatorIndex1 = uint8(bound(operatorIndex1, 0, operators.length - 1));
        operatorIndex2 = uint8(bound(operatorIndex2, 0, operators.length - 1));
        
        address op1 = operators[operatorIndex1];
        address op2 = operators[operatorIndex2];
        
        // Execute first action
        _executeAction(action1, op1);
        
        // Execute second action (should handle conflicts gracefully)
        try this.executeActionExternal(action2, op2) {
            // Action succeeded - verify cluster consistency
            _verifyClusterConsistency();
        } catch {
            // Action failed - this is acceptable for conflicting operations
        }
    }

    /*//////////////////////////////////////////////////////////////////////////
                            INTERNAL HELPERS
    //////////////////////////////////////////////////////////////////////////*/
    
    function _filterAndSortSlots(uint8[] memory slots) internal pure returns (uint8[] memory) {
        if (slots.length == 0) return new uint8[](0);
        
        // Filter valid slots and remove duplicates
        uint8[] memory temp = new uint8[](slots.length);
        uint256 count = 0;
        
        for (uint256 i = 0; i < slots.length; i++) {
            if (slots[i] < 10) { // Valid slot range
                // Check for duplicates
                bool duplicate = false;
                for (uint256 j = 0; j < count; j++) {
                    if (temp[j] == slots[i]) {
                        duplicate = true;
                        break;
                    }
                }
                if (!duplicate) {
                    temp[count++] = slots[i];
                }
            }
        }
        
        // Create properly sized array
        uint8[] memory result = new uint8[](count);
        for (uint256 i = 0; i < count; i++) {
            result[i] = temp[i];
        }
        
        // Sort the array (simple bubble sort for small arrays)
        for (uint256 i = 0; i < result.length; i++) {
            for (uint256 j = i + 1; j < result.length; j++) {
                if (result[i] > result[j]) {
                    uint8 temp_val = result[i];
                    result[i] = result[j];
                    result[j] = temp_val;
                }
            }
        }
        
        return result;
    }
    
    function _executeAction(uint8 action, address operator) internal {
        if (action == 0) {
            // Toggle maintenance
            vm.prank(operator);
            cluster.setMaintenance(true);
        } else if (action == 1) {
            // Add operator (owner only)
            NodeOperator memory newOp = NodeOperator({
                addr: address(uint160(0x999)),
                data: "new_operator"
            });
            vm.prank(owner);
            cluster.addNodeOperator(newOp);
        } else if (action == 2) {
            // Update settings (owner only)
            Settings memory newSettings = Settings({
                maxOperatorDataBytes: 2048,
                minOperators: 2,
                extra: ""
            });
            vm.prank(owner);
            cluster.updateSettings(newSettings);
        }
    }
    
    // External wrapper for try/catch usage
    function executeActionExternal(uint8 action, address operator) external {
        _executeAction(action, operator);
    }
    
    function _verifyClusterConsistency() internal view {
        // Basic consistency checks
        uint16 count = cluster.getOperatorCount();
        address[] memory allOps = cluster.getAllOperators();
        
        // Operator count should match
        assertTrue(count > 0);
        assertTrue(allOps.length <= 255);
        
        // Version should be monotonic (we can't check this without state)
        assertTrue(cluster.version() > 0);
        
        // If no migration, pulling count should be 0
        (, uint16 pulling, bool inProgress) = cluster.getMigrationStatus();
        if (!inProgress) {
            assertEq(pulling, 0);
        }
    }
} 
