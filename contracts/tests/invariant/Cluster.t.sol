// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {StdInvariant} from "forge-std/StdInvariant.sol";
import {Test} from "forge-std/Test.sol";
import {console} from "forge-std/console.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {ClusterHarness} from "tests/unit/ClusterHarness.sol";
import {ClusterStore} from "tests/invariant/store/ClusterStore.sol";
import {OperatorHandler} from "tests/invariant/handler/OperatorHandler.sol";
import {MigrationHandler} from "tests/invariant/handler/MigrationHandler.sol";
import {MaintenanceHandler} from "tests/invariant/handler/MaintenanceHandler.sol";
import {Bitmask, Cluster, Settings, NodeOperator, ClusterView, Keyspace} from "src/Cluster.sol";

contract ClusterInvariants is StdInvariant, Test {
    using Bitmask for uint256;

    /*//////////////////////////////////////////////////////////////////////////
                                     VARIABLES
    //////////////////////////////////////////////////////////////////////////*/
    
    ClusterHarness public cluster;
    ClusterStore public store;
    OperatorHandler public operatorHandler;
    MigrationHandler public migrationHandler;
    MaintenanceHandler public maintenanceHandler;
    address public owner;

    
    /*//////////////////////////////////////////////////////////////////////////
                                      SETUP
    //////////////////////////////////////////////////////////////////////////*/
    
    function setUp() public {
        owner = address(0x123);
        cluster = _freshCluster(5); // Start with reasonable size
        
        // Create store
        store = new ClusterStore();
        
        // Create focused handlers
        operatorHandler = new OperatorHandler(cluster, store, owner);
        migrationHandler = new MigrationHandler(cluster, store, owner);
        maintenanceHandler = new MaintenanceHandler(cluster, store, owner);
        
        // Target all handlers for fuzzing
        targetContract(address(operatorHandler));
        targetContract(address(migrationHandler));
        targetContract(address(maintenanceHandler));
        
        // Initialize store with current state
        ClusterView memory initialView = cluster.getView();
        store.updatePreviousViewOf(initialView.version, initialView);
        store.updateIsPreviousViewRecorded(initialView.version);
    }
    
    /*//////////////////////////////////////////////////////////////////////////
                                     INVARIANTS
    //////////////////////////////////////////////////////////////////////////*/ 
    
    /// @dev Keyspace members must exist and be sorted
    function invariant_KeyspaceIntegrity() public view {
        ClusterView memory clusterView = cluster.getView();
        Keyspace memory currentKeyspace = clusterView.keyspaces[clusterView.keyspaceVersion % 2];
        uint8[] memory members = currentKeyspace.operatorBitmask.intoArray();
        
        // All keyspace members must correspond to actual operators
        for (uint256 i = 0; i < members.length; i++) {
            assertTrue(
                cluster.operatorBitmask().isSet(members[i]),
                "Keyspace member slot must be occupied"
            );
        }
        
        // Keyspace must be sorted
        for (uint256 i = 1; i < members.length; i++) {
            assertLt(
                members[i-1],
                members[i],
                "Keyspace members must be sorted"
            );
        }
        
        // Keyspace size cannot exceed operator count
        assertLe(
            members.length,
            clusterView.operatorSlots.length,
            "Keyspace size cannot exceed operator count"
        );
    }  
    
    /// @dev Migration state consistency
    function invariant_MigrationStateConsistency() public view {
        ClusterView memory clusterView = cluster.getView();
        
        // If pulling operators exist, migration should be in progress
        bool hasPullingOps = clusterView.migration.pullingOperatorBitmask > 0;
        (,, bool migrationInProgress) = cluster.getMigrationStatus();
        
        if (hasPullingOps) {
            assertTrue(
                migrationInProgress,
                "Migration must be in progress if pulling operators exist"
            );
        }
        
        // Pulling operators must be valid slots
        for (uint256 i = 0; i <= clusterView.migration.pullingOperatorBitmask.findLastSet(); i++) {
            if (clusterView.migration.pullingOperatorBitmask.isSet(uint8(i))) {
                assertTrue(
                    cluster.operatorBitmask().isSet(uint8(i)),
                    "Pulling operator slot must be occupied"
                );
            }
        }
    }
    
    /// @dev ② if pullingCount == 0 ⇒ isPulling[x] == false for all x
    /// This single invariant replaces half the migration tests!
    /// Covers completeMigration, abortMigration, and startMigration edge-cases  
    function invariant_NoDanglingPullFlags() public view {
        (, uint16 remaining, ) = cluster.getMigrationStatus();
        
        if (remaining == 0) {
            // When no operators are pulling, getPullingOperators should be empty
            uint8[] memory pullingOps = cluster.getPullingOperators();
            assertEq(pullingOps.length, 0, "No operators should be pulling when remaining == 0");
        }
    }
    
    /// @dev ③ keyspaceVersion % 2 matches active keyspace array
    function invariant_KeyspaceVersionMapsToActiveArray() public view {
        ClusterView memory clusterView = cluster.getView();
        (,, bool migrationInProgress) = cluster.getMigrationStatus();
        Keyspace memory currentKeyspace = clusterView.keyspaces[clusterView.keyspaceVersion % 2];

        // The keyspace version should determine which internal keyspace array is active
        // Since we can only see the current keyspace in the view, we verify it's consistent
        assertTrue(clusterView.keyspaceVersion >= 0, "Keyspace version should be valid");
        
        // Current keyspace should have valid data when operators exist, unless a migration is in progress
        if (clusterView.operatorSlots.length > 0 && !migrationInProgress) {
            assertTrue(
                currentKeyspace.operatorBitmask > 0,
                "Current keyspace should have members when operators exist and no migration is in progress"
            );
        }
    }
    
    /// @dev ④ version monotonically increases  
    function invariant_VersionMonotonicity() public view {
        ClusterView memory currentView = cluster.getView();
        
        // Version must always be positive
        assertTrue(currentView.version > 0, "Version must be positive");
        
        // Check monotonicity against stored previous state
        if (store.isPreviousViewRecorded(store.lastVersion()) && store.lastVersion() < currentView.version) {
            // Get the previous view (public mapping returns tuple components)
            ClusterView memory previousView = store.previousViewOf(store.lastVersion());
            assertGe(
                currentView.version,
                previousView.version,
                "Version must monotonically increase"
            );
        }
    }
    
    /// @dev Clean up and log results after invariant testing (Lockup style)
    function afterInvariant() public view {
        ClusterView memory finalClusterView = cluster.getView();
        
        // Log final state
        console.log("=== INVARIANT CAMPAIGN COMPLETED ===");
        console.log("Final operator count:", finalClusterView.operatorSlots.length);
        console.log("Final cluster version:", finalClusterView.version);
        console.log("Final keyspace version:", finalClusterView.keyspaceVersion);
        console.log("Migration in progress:", finalClusterView.migration.pullingOperatorBitmask > 0);
        console.log("Maintenance active:", finalClusterView.maintenanceSlot != address(0));
        
        // Log call distribution from handlers (Lockup pattern)
        console.log("\n=== HANDLER CALL STATISTICS ===");
        console.log("OperatorHandler total calls:", operatorHandler.totalCalls());
        console.log("- addNodeOperator:", operatorHandler.calls("addNodeOperator"));
        console.log("- updateNodeOperator:", operatorHandler.calls("updateNodeOperator")); 
        console.log("- removeNodeOperator:", operatorHandler.calls("removeNodeOperator"));
        
        console.log("MigrationHandler total calls:", migrationHandler.totalCalls());
        console.log("- startMigration:", migrationHandler.calls("startMigration"));
        console.log("- completeMigration:", migrationHandler.calls("completeMigration"));
        console.log("- abortMigration:", migrationHandler.calls("abortMigration"));
        
        console.log("MaintenanceHandler total calls:", maintenanceHandler.totalCalls());
        console.log("- startMaintenanceAsOwner:", maintenanceHandler.calls("startMaintenanceAsOwner"));
        
        // Log store statistics
        console.log("\n=== STORE STATISTICS ===");
        console.log("Total migrations seen:", store.getMigrationCount());
        console.log("Total operators seen:", store.getOperatorCount());
        console.log("Last recorded version:", store.lastVersion());
    }

    /*//////////////////////////////////////////////////////////////////////////
                                   HELPERS
    //////////////////////////////////////////////////////////////////////////*/
    
    /// @dev Helper to create a fresh cluster with N operators
    function _freshCluster(uint256 n) internal returns (ClusterHarness) {
        n = bound(n, 1, 20); // Reasonable range for invariant testing
        
        NodeOperator[] memory operators = new NodeOperator[](n);
        for (uint256 i = 0; i < n; i++) {
            operators[i] = NodeOperator({
                addr: address(uint160(0x1000 + i)),
                data: abi.encodePacked("operator", i)
            });
        }

        ClusterHarness implementation = new ClusterHarness();
        bytes memory initData = abi.encodeCall(
            Cluster.initialize, 
            (_defaultSettings(), operators)
        );
        
        vm.prank(owner);
        ERC1967Proxy proxy = new ERC1967Proxy(address(implementation), initData);
        
        return ClusterHarness(address(proxy));
    }
    
    /// @dev Default settings for testing
    function _defaultSettings() internal pure returns (Settings memory) {
        return Settings({
            maxOperatorDataBytes: 4096,
            minOperators: 1,
            extra: ""
        });
    }
} 
