// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "forge-std/Test.sol";
import {Vm} from "forge-std/Vm.sol";

import {Cluster, Settings, NodeOperator, NodeOperatorData} from "../src/Cluster.sol";
import {Ownable} from "@openzeppelin-contracts/access/Ownable.sol";
import {LibSort} from "solady/utils/LibSort.sol";

import "@openzeppelin-contracts/utils/Strings.sol";





contract ClusterTest is Test {
    /*//////////////////////////////////////////////////////////////////////////
                                CONSTANTS
    //////////////////////////////////////////////////////////////////////////*/

    
    address constant OWNER = address(12345);
    address constant NEW_OWNER = address(56789);
    address constant ANYONE = address(9000);
    address OPERATOR;
    bytes constant DEFAULT_OPERATOR_DATA = "Some operator specific data";
    uint16 constant MAX_OPERATOR_DATA_BYTES = 4096;

    /*//////////////////////////////////////////////////////////////////////////
                                EVENTS
    //////////////////////////////////////////////////////////////////////////*/

    event ClusterInitialized(uint256 operatorCount, Settings settings, uint128 version);
    event MigrationStarted(uint64 indexed id, address[] operators, uint8 replicationStrategy, uint64 keyspaceVersion, uint128 version);
    event MigrationDataPullCompleted(uint64 indexed id, address indexed operator, uint128 version);
    event MigrationCompleted(uint64 indexed id, address indexed operator, uint128 version);
    event MigrationAborted(uint64 indexed id, uint128 version);

    event NodeOperatorAdded(address indexed operator, NodeOperatorData operatorData, uint128 version);
    event NodeOperatorUpdated(address indexed operator, NodeOperatorData operatorData, uint128 version);
    event NodeOperatorRemoved(address indexed operator, uint128 version);

    event MaintenanceToggled(address indexed operator, bool active, uint128 version);
    event SettingsUpdated(Settings newSettings, address indexed updatedBy, uint128 version);

    using Strings for uint256;
    using LibSort for *;

    Cluster cluster;

    function setUp() public {
        OPERATOR = vm.addr(1); // This matches the first operator in the cluster
        newCluster();
    }

    function test_SetUpState() public {
        assertEq(cluster.getOperatorCount(), 5, "Initial operator count");
        assertTrue(cluster.isInitialized(), "Cluster should be initialized");
        assertEq(cluster.version(), 0, "Initial version should be 0");
        assertEq(cluster.keyspaceVersion(), 0, "Initial keyspace version should be 0");
    }

    /*//////////////////////////////////////////////////////////////////////////
                            MODIFIERS
    //////////////////////////////////////////////////////////////////////////*/

    modifier asAnyone() {
        vm.startPrank(ANYONE);
        _;
        vm.stopPrank();
    }

    modifier asOperator() {
        vm.startPrank(OPERATOR);
        _;
        vm.stopPrank();
    }

    modifier asOwner() {
        vm.startPrank(OWNER);
        _;
        vm.stopPrank();
    }

    modifier asNewOwner() {
        vm.startPrank(NEW_OWNER);
        _;
        vm.stopPrank();
    }

    /*//////////////////////////////////////////////////////////////////////////
                            CONSTRUCTOR TESTS
    //////////////////////////////////////////////////////////////////////////*/

    function test_RevertWhen_TooManyOperators() public {
        vm.startPrank(OWNER);
        vm.expectRevert(Cluster.TooManyOperators.selector);
        
        NodeOperator[] memory operators = new NodeOperator[](257);
        for (uint256 i = 0; i < 257; i++) {
            operators[i] = newNodeOperator(i + 1, DEFAULT_OPERATOR_DATA);
        }
        
        cluster = new Cluster(Settings({ maxOperatorDataBytes: MAX_OPERATOR_DATA_BYTES }), operators);
        vm.stopPrank();
    }

    function test_Constructor_MaxNumberOfOperators() public {
        newCluster(256);
        assertEq(cluster.getOperatorCount(), 256, "Should allow max operators");
    }

    function test_Constructor_ContainsInitialNodeOperators() public {
        assertEq(cluster.getOperatorCount(), 5, "Initial operator count");
        for (uint256 i = 0; i < 5; i++) {
            address expectedAddr = vm.addr(i + 1);
            assertTrue(cluster.isOperator(expectedAddr), "Operator should exist");
            (address addr, bytes memory data, bool maintenance) = cluster.info(expectedAddr);
            assertEq(addr, expectedAddr, "Operator address");
            assertEq(data, DEFAULT_OPERATOR_DATA, "Operator data");
            assertFalse(maintenance, "Initial maintenance state");
        }
    }

    function test_Constructor_InitialKeyspace() public {
        (address[] memory operators, uint8 replicationStrategy) = cluster.getCurrentKeyspace();
        assertEq(operators.length, 5, "Keyspace operator count");
        assertEq(replicationStrategy, 0, "Initial replication strategy");
        // Keyspace should be sorted
        for (uint256 i = 1; i < operators.length; i++) {
            assertTrue(operators[i-1] < operators[i], "Keyspace should be sorted");
        }
    }

    function test_Constructor_EmitsInitializedEvent() public {
        Settings memory expectedSettings = Settings({ maxOperatorDataBytes: MAX_OPERATOR_DATA_BYTES });
        
        vm.expectEmit(true, true, true, true);
        emit ClusterInitialized(3, expectedSettings, 0);
        newCluster(3);
    }

    function test_Constructor_EmptyClusterNotInitialized() public {
        newCluster(0);
        assertFalse(cluster.isInitialized(), "Empty cluster should not be initialized");
    }

    /*//////////////////////////////////////////////////////////////////////////
                            ADD NODE OPERATOR
    //////////////////////////////////////////////////////////////////////////*/


    
    function test_RevertWhen_NonOwnerAddsOperator() public asAnyone {
        uint256 newOperatorKey = 6;
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, ANYONE));
        cluster.addNodeOperator(newNodeOperator(newOperatorKey));
    }

    function test_RevertWhen_OperatorAddsAnotherOperator() public asOperator {
        uint256 newOperatorKey = 6;
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, OPERATOR));
        cluster.addNodeOperator(newNodeOperator(newOperatorKey));
    }

    function test_AddNodeOperator_Success() public asOwner {
        uint256 newOperatorKey = 6;
        address newOperatorAddr = vm.addr(newOperatorKey);
        cluster.addNodeOperator(newNodeOperator(newOperatorKey));
        assertTrue(cluster.isOperator(newOperatorAddr), "Operator should be added");
        assertEq(cluster.getOperatorCount(), 6, "Operator count should increase");
    }

    function test_AddNodeOperator_BumpsVersion() public asOwner {
        uint256 newOperatorKey = 6;
        cluster.addNodeOperator(newNodeOperator(newOperatorKey));
        assertEq(cluster.version(), 1, "Version should increment");
    }

    function test_AddNodeOperator_DoesNotBumpKeyspaceVersion() public asOwner {
        uint256 newOperatorKey = 6;
        cluster.addNodeOperator(newNodeOperator(newOperatorKey));
        assertEq(cluster.keyspaceVersion(), 0, "Keyspace version should not change");
    }

    function test_AddNodeOperator_EmitsEvent() public asOwner {
        uint256 newOperatorKey = 6;
        NodeOperator memory operator = newNodeOperator(newOperatorKey);
        vm.expectEmit(true, true, true, true);
        emit NodeOperatorAdded(operator.addr, NodeOperatorData({data: operator.data, maintenance: operator.maintenance}), 1);
        cluster.addNodeOperator(operator);
    }

    function test_RevertWhen_AddingDuplicateOperator() public asOwner {
        uint256 existingOperatorKey = 1;
        vm.expectRevert(Cluster.OperatorExists.selector);
        cluster.addNodeOperator(newNodeOperator(existingOperatorKey)); // operator 1 already exists
    }

    /*//////////////////////////////////////////////////////////////////////////
                            UPDATE NODE OPERATOR
    //////////////////////////////////////////////////////////////////////////*/

    function test_RevertWhen_NonOwnerUpdatesOperator() public asAnyone {
        uint256 operatorKey = 1;
        vm.expectRevert(Cluster.Unauthorized.selector);
        cluster.updateNodeOperator(newNodeOperator(operatorKey, "new data"));
    }

    function test_UpdateNodeOperator_OwnerCanUpdate() public asOwner {
        uint256 operatorKey = 1;
        cluster.updateNodeOperator(newNodeOperator(operatorKey, "new data"));
        (, bytes memory data,) = cluster.info(vm.addr(operatorKey));
        assertEq(data, "new data", "Data should be updated");
    }

    function test_RevertWhen_OperatorUpdatesAnother() public asOperator {
        uint256 otherOperatorKey = 2;
        vm.expectRevert(Cluster.Unauthorized.selector);
        cluster.updateNodeOperator(newNodeOperator(otherOperatorKey, "new data"));
    }

    function test_UpdateNodeOperator_OperatorCanUpdateSelf() public asOperator {
        uint256 operatorKey = 1;
        cluster.updateNodeOperator(newNodeOperator(operatorKey, "new data"));
        (, bytes memory data,) = cluster.info(vm.addr(operatorKey));
        assertEq(data, "new data", "Operator should update own data");
    }

    function test_RevertWhen_UpdateOnUninitializedCluster() public {
        uint256 operatorKey = 1;
        newCluster(0);
        vm.expectRevert(Cluster.NotInitialized.selector);
        vm.startPrank(OWNER);
        cluster.updateNodeOperator(newNodeOperator(operatorKey, "new data"));
        vm.stopPrank();
    }

    function test_UpdateNodeOperator_UpdatesData() public asOwner {
        uint256 operatorKey = 1;
        address operatorAddr = vm.addr(operatorKey);
        NodeOperator memory operator = newNodeOperator(operatorKey, "new data");
        cluster.updateNodeOperator(operator);
        (address addr, bytes memory data, bool maintenance) = cluster.info(operatorAddr);
        assertEq(addr, operatorAddr, "Address should match");
        assertEq(data, operator.data, "Data should be updated");
        assertFalse(maintenance, "Maintenance state");
    }

    function test_UpdateNodeOperator_BumpsVersion() public asOwner {
        uint256 operatorKey = 1;
        cluster.updateNodeOperator(newNodeOperator(operatorKey, "new data"));
        assertEq(cluster.version(), 1, "Version should increment");
    }

    function test_UpdateNodeOperator_DoesNotBumpKeyspaceVersion() public asOwner {
        uint256 operatorKey = 1;
        cluster.updateNodeOperator(newNodeOperator(operatorKey, "new data"));
        assertEq(cluster.keyspaceVersion(), 0, "Keyspace version should not change");
    }

    function test_UpdateNodeOperator_EmitsEvent() public asOwner {
        uint256 operatorKey = 1;
        NodeOperator memory operator = newNodeOperator(operatorKey, "new data");
        vm.expectEmit(true, true, true, true);
        emit NodeOperatorUpdated(operator.addr, NodeOperatorData({data: operator.data, maintenance: operator.maintenance}), 1);
        cluster.updateNodeOperator(operator);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            REMOVE NODE OPERATOR
    //////////////////////////////////////////////////////////////////////////*/

    function test_RevertWhen_NonOwnerRemovesOperator() public asAnyone {
        uint256 operatorKey = 1;
        address operatorAddr = vm.addr(operatorKey);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, ANYONE));
        cluster.removeNodeOperator(operatorAddr);
    }

    function test_RevertWhen_OperatorRemovesAnother() public asOperator {
        uint256 operatorKey = 1;
        uint256 otherOperatorKey = 2;
        address operatorAddr = vm.addr(operatorKey);
        address otherOperatorAddr = vm.addr(otherOperatorKey);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, OPERATOR));
        cluster.removeNodeOperator(otherOperatorAddr);
    }

    function test_RevertWhen_OperatorRemovesSelf() public asOperator {
        uint256 operatorKey = 1;
        address operatorAddr = vm.addr(operatorKey);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, OPERATOR));
        cluster.removeNodeOperator(operatorAddr);
    }

    function test_RemoveNodeOperator_Success() public asOwner {
        uint256 newOperatorKey = 6;
        address newOperatorAddr = vm.addr(newOperatorKey);
        cluster.addNodeOperator(newNodeOperator(newOperatorKey));
        cluster.removeNodeOperator(newOperatorAddr);
        assertFalse(cluster.isOperator(newOperatorAddr), "Operator should be removed");
    }

    function test_RevertWhen_RemoveOperatorInKeyspace() public asOwner {
        uint256 operatorKey = 1;
        address operatorAddr = vm.addr(operatorKey);
        vm.expectRevert(Cluster.OperatorInKeyspace.selector);
        cluster.removeNodeOperator(operatorAddr);
    }

    function test_RevertWhen_RemoveNonExistentOperator() public asOwner {
        address nonExistentOperator = vm.addr(999);
        vm.expectRevert(Cluster.OperatorNotFound.selector);
        cluster.removeNodeOperator(nonExistentOperator);
    }

    function test_RevertWhen_RemoveOperatorDuringMigration() public asOwner {
        // Add a new operator first
        uint256 newOperatorKey = 6;
        address newOperatorAddr = vm.addr(newOperatorKey);
        cluster.addNodeOperator(newNodeOperator(newOperatorKey));
        
        // Start migration including this operator
        address[] memory operators = new address[](6);
        for (uint256 i = 0; i < 6; i++) {
            operators[i] = vm.addr(i + 1);
        }
        operators.sort();
        cluster.startMigration(operators, 0);
        
        // Try to remove the operator - should fail because it's in the new keyspace
        vm.expectRevert(Cluster.OperatorInKeyspace.selector);
        cluster.removeNodeOperator(newOperatorAddr);
    }

    function test_RemoveNodeOperator_UpdatesCount() public asOwner {
        uint256 newOperatorKey = 6;
        address newOperatorAddr = vm.addr(newOperatorKey);
        cluster.addNodeOperator(newNodeOperator(newOperatorKey));
        assertEq(cluster.getOperatorCount(), 6, "Count after add");
        assertTrue(cluster.isOperator(newOperatorAddr), "Operator exists");
        
        cluster.removeNodeOperator(newOperatorAddr);
        assertEq(cluster.getOperatorCount(), 5, "Count after remove");
        assertFalse(cluster.isOperator(newOperatorAddr), "Operator removed");
    }

    function test_RemoveNodeOperator_BumpsVersion() public asOwner {
        uint256 newOperatorKey = 6;
        address newOperatorAddr = vm.addr(newOperatorKey);
        cluster.addNodeOperator(newNodeOperator(newOperatorKey));
        uint128 versionBefore = cluster.version();
        cluster.removeNodeOperator(newOperatorAddr);
        assertEq(cluster.version(), versionBefore + 1, "Version should increment");
    }

    function test_RemoveNodeOperator_DoesNotBumpKeyspaceVersion() public asOwner {
        uint256 newOperatorKey = 6;
        address newOperatorAddr = vm.addr(newOperatorKey);
        cluster.addNodeOperator(newNodeOperator(newOperatorKey));
        cluster.removeNodeOperator(newOperatorAddr);
        assertEq(cluster.keyspaceVersion(), 0, "Keyspace version should not change");
    }

    function test_RemoveNodeOperator_EmitsEvent() public asOwner {
        uint256 newOperatorKey = 6;
        address newOperatorAddr = vm.addr(newOperatorKey);
        cluster.addNodeOperator(newNodeOperator(newOperatorKey));
        vm.expectEmit(true, true, true, true);
        emit NodeOperatorRemoved(newOperatorAddr, 2);
        cluster.removeNodeOperator(newOperatorAddr);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            START MIGRATION
    //////////////////////////////////////////////////////////////////////////*/

    function test_RevertWhen_NonOwnerStartsMigration() public asAnyone {
        address[] memory operators = getSortedOperatorArray(4);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, ANYONE));
        cluster.startMigration(operators, 0);
    }

    function test_RevertWhen_OperatorStartsMigration() public asOperator {
        address[] memory operators = getSortedOperatorArray(4);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, OPERATOR));
        cluster.startMigration(operators, 0);
    }

    function test_StartMigration_Success() public asOwner {
        address[] memory operators = getSortedOperatorArray(4);
        cluster.startMigration(operators, 0);
        
        (uint64 id, uint16 remaining, bool inProgress) = cluster.getMigrationStatus();
        assertEq(id, 1, "Migration ID");
        assertEq(remaining, 4, "Remaining operators");
        assertTrue(inProgress, "Migration in progress");
    }

    function test_RevertWhen_StartMigrationOnUninitializedCluster() public {
        newCluster(0);
        address[] memory operators = new address[](0);
        vm.expectRevert(Cluster.NotInitialized.selector);
        vm.startPrank(OWNER);
        cluster.startMigration(operators, 0);
    }

    function test_RevertWhen_MigrationWithTooManyOperators() public asOwner {
        address[] memory operators = new address[](257);
        for (uint256 i = 0; i < 257; i++) {
            operators[i] = vm.addr(i + 1);
        }
        vm.expectRevert(Cluster.TooManyOperators.selector);
        cluster.startMigration(operators, 0);
    }

    function test_MigrationWith256Operators() public asOwner {
        // Create a cluster with 256 operators
        NodeOperator[] memory initialOperators = new NodeOperator[](256);
        for (uint256 i = 0; i < 256; i++) {
            initialOperators[i] = newNodeOperator(i + 1, DEFAULT_OPERATOR_DATA);
        }
        
        Cluster maxCluster = new Cluster(Settings({ maxOperatorDataBytes: MAX_OPERATOR_DATA_BYTES }), initialOperators);
        
        // Get all operators (should be sorted)
        address[] memory allOperators = maxCluster.getAllOperators();
        assertEq(allOperators.length, 256, "Should have 256 operators");
        
        // Verify initial keyspace has replication strategy 0
        (, uint8 initialStrategy) = maxCluster.getCurrentKeyspace();
        assertEq(initialStrategy, 0, "Initial replication strategy should be 0");
        
        // Migrate all 256 operators with a different replication strategy
        // This should succeed because replication strategy is different
        maxCluster.startMigration(allOperators, 1);
        
        (uint64 id, uint16 remaining, bool inProgress) = maxCluster.getMigrationStatus();
        assertEq(id, 1, "Migration ID");
        assertEq(remaining, 256, "All 256 operators should be pulling");
        assertTrue(inProgress, "Migration should be in progress");
        
        // Verify new keyspace has different replication strategy
        (address[] memory newOperators, uint8 newStrategy) = maxCluster.getCurrentKeyspace();
        assertEq(newOperators.length, 256, "Should have 256 operators in new keyspace");
        assertEq(newStrategy, 1, "New replication strategy should be 1");
    }

    function test_RevertWhen_UnsortedOperators() public asOwner {
        address[] memory operators = new address[](3);
        operators[0] = vm.addr(3); // unsorted
        operators[1] = vm.addr(1);
        operators[2] = vm.addr(2);
        
        vm.expectRevert(Cluster.InvalidOperator.selector);
        cluster.startMigration(operators, 0);
    }

    function test_RevertWhen_DuplicateOperators() public asOwner {
        address[] memory operators = new address[](3);
        operators[0] = vm.addr(1);
        operators[1] = vm.addr(1); // duplicate
        operators[2] = vm.addr(2);
        
        vm.expectRevert(Cluster.InvalidOperator.selector);
        cluster.startMigration(operators, 0);
    }

    function test_RevertWhen_MigrationInProgress() public asOwner {
        address[] memory operators = getSortedOperatorArray(4);
        cluster.startMigration(operators, 0);
        
        vm.expectRevert(Cluster.MigrationInProgress.selector);
        cluster.startMigration(operators, 1);
    }

    function test_RevertWhen_SameMigration() public asOwner {
        address[] memory operators = getSortedOperatorArray(5);
        vm.expectRevert(Cluster.SameKeyspace.selector);
        cluster.startMigration(operators, 0);
    }

    function test_StartMigration_DifferentReplicationStrategy() public asOwner {
        // Same operators but different replication strategy should succeed
        address[] memory operators = getSortedOperatorArray(5);
        cluster.startMigration(operators, 1); // Different replication strategy
        
        (address[] memory keyspaceOperators, uint8 replicationStrategy) = cluster.getCurrentKeyspace();
        assertEq(keyspaceOperators.length, 5, "Should have 5 operators");
        assertEq(replicationStrategy, 1, "Replication strategy should be 1");
    }

    function test_StartMigration_EmptyMigration() public asOwner {
        // Starting migration with empty array should be allowed and change keyspace to empty
        address[] memory operators = new address[](0);
        cluster.startMigration(operators, 0);
        
        (address[] memory keyspaceOperators, uint8 replicationStrategy) = cluster.getCurrentKeyspace();
        assertEq(keyspaceOperators.length, 0, "Keyspace should be empty");
        assertEq(replicationStrategy, 0, "Replication strategy should be 0");
    }

    function test_StartMigration_BumpsVersion() public asOwner {
        address[] memory operators = getSortedOperatorArray(4);
        cluster.startMigration(operators, 0);
        assertEq(cluster.version(), 1, "Version should increment");
    }

    function test_StartMigration_BumpsKeyspaceVersion() public asOwner {
        address[] memory operators = getSortedOperatorArray(4);
        cluster.startMigration(operators, 0);
        assertEq(cluster.keyspaceVersion(), 1, "Keyspace version should increment");
    }

    function test_StartMigration_EmitsEvent() public asOwner {
        address[] memory operators = getSortedOperatorArray(4);
        vm.expectEmit(true, true, true, true);
        emit MigrationStarted(1, operators, 0, 1, 1);
        cluster.startMigration(operators, 0);
    }

    function test_RevertWhen_InvalidOperatorInMigration() public asOwner {
        address[] memory operators = new address[](2);
        operators[0] = vm.addr(1);
        operators[1] = vm.addr(999); // doesn't exist
        
        vm.expectRevert(Cluster.OperatorNotFound.selector);
        cluster.startMigration(operators, 0);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            COMPLETE MIGRATION
    //////////////////////////////////////////////////////////////////////////*/

    function test_RevertWhen_NonOperatorCompletesMigration() public asAnyone {
        address[] memory operators = getSortedOperatorArray(4);
        vm.stopPrank();
        vm.startPrank(OWNER);
        cluster.startMigration(operators, 0);
        vm.stopPrank();
        
        vm.startPrank(ANYONE);
        vm.expectRevert(Cluster.CallerNotOperator.selector);
        cluster.completeMigration(1);
    }

    function test_RevertWhen_OwnerCompletesMigration() public asOwner {
        address[] memory operators = getSortedOperatorArray(4);
        cluster.startMigration(operators, 0);
        
        vm.expectRevert(Cluster.CallerNotOperator.selector);
        cluster.completeMigration(1);
    }

    function test_RevertWhen_CompleteNonExistentMigration() public asOperator {
        vm.expectRevert(Cluster.NoMigrationInProgress.selector);
        cluster.completeMigration(1);
    }

    function test_RevertWhen_CompleteMigrationOnUninitializedCluster() public {
        newCluster(0);
        vm.expectRevert(Cluster.NotInitialized.selector);
        vm.startPrank(OPERATOR);
        cluster.completeMigration(1);
        vm.stopPrank();
    }

    function test_CompleteMigration_Success() public asOwner {
        address[] memory operators = getSortedOperatorArray(4);
        cluster.startMigration(operators, 0);
        vm.stopPrank();
        
        vm.startPrank(vm.addr(1));
        cluster.completeMigration(1);
        vm.stopPrank();
        
        (,uint16 remaining,) = cluster.getMigrationStatus();
        assertEq(remaining, 3, "Remaining operators should decrease");
    }

    function test_RevertWhen_CompleteMigrationTwice() public asOwner {
        address[] memory operators = getSortedOperatorArray(4);
        cluster.startMigration(operators, 0);
        vm.stopPrank();
        
        vm.startPrank(OPERATOR);
        cluster.completeMigration(1);
        
        vm.expectRevert(Cluster.OperatorNotPulling.selector);
        cluster.completeMigration(1);
        vm.stopPrank();
    }

    function test_CompleteMigration_BumpsVersion() public asOwner {
        address[] memory operators = getSortedOperatorArray(4);
        cluster.startMigration(operators, 0);
        vm.stopPrank();
        
        vm.startPrank(vm.addr(1));
        cluster.completeMigration(1);
        vm.stopPrank();
        
        assertEq(cluster.version(), 2, "Version should increment");
    }

    function test_CompleteMigration_EmitsDataPullEvent() public asOwner {
        address[] memory operators = getSortedOperatorArray(4);
        cluster.startMigration(operators, 0);
        vm.stopPrank();
        
        vm.startPrank(vm.addr(1));
        vm.expectEmit(true, true, true, true);
        emit MigrationDataPullCompleted(1, vm.addr(1), 2);
        cluster.completeMigration(1);
        vm.stopPrank();
    }

    function test_CompleteMigration_EmitsCompletedEvent() public asOwner {
        address[] memory operators = getSortedOperatorArray(2);
        
        cluster.startMigration(operators, 0);
        vm.stopPrank();
        
        vm.startPrank(vm.addr(1));
        cluster.completeMigration(1);
        vm.stopPrank();
        
        vm.startPrank(vm.addr(2));
        vm.expectEmit(true, true, true, true);
        emit MigrationCompleted(1, vm.addr(2), 3);
        cluster.completeMigration(1);
        vm.stopPrank();
    }

    /*//////////////////////////////////////////////////////////////////////////
                            ABORT MIGRATION
    //////////////////////////////////////////////////////////////////////////*/

    function test_RevertWhen_NonOwnerAbortsMigration() public asOwner {
        address[] memory operators = getSortedOperatorArray(4);
        cluster.startMigration(operators, 0);
        vm.stopPrank();
        
        vm.startPrank(ANYONE);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, ANYONE));
        cluster.abortMigration(1);
        vm.stopPrank();
    }

    function test_RevertWhen_OperatorAbortsMigration() public asOwner {
        address[] memory operators = getSortedOperatorArray(4);
        cluster.startMigration(operators, 0);
        vm.stopPrank();
        
        vm.startPrank(OPERATOR);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, OPERATOR));
        cluster.abortMigration(1);
        vm.stopPrank();
    }

    function test_AbortMigration_Success() public asOwner {
        address[] memory operators = getSortedOperatorArray(4);
        cluster.startMigration(operators, 0);
        cluster.abortMigration(1);
        
        (,, bool inProgress) = cluster.getMigrationStatus();
        assertFalse(inProgress, "Migration should not be in progress");
    }

    function test_RevertWhen_AbortNonExistentMigration() public asOwner {
        vm.expectRevert(Cluster.NoMigrationInProgress.selector);
        cluster.abortMigration(1);
    }

    function test_AbortMigration_BumpsVersion() public asOwner {
        address[] memory operators = getSortedOperatorArray(4);
        cluster.startMigration(operators, 0);
        cluster.abortMigration(1);
        assertEq(cluster.version(), 2, "Version should increment");
    }

    function test_AbortMigration_RevertsKeyspaceVersion() public asOwner {
        assertEq(cluster.keyspaceVersion(), 0, "Initial keyspace version");
        
        address[] memory operators = getSortedOperatorArray(4);
        cluster.startMigration(operators, 0);
        assertEq(cluster.keyspaceVersion(), 1, "Keyspace version after start");
        
        cluster.abortMigration(1);
        assertEq(cluster.keyspaceVersion(), 0, "Keyspace version after abort");
    }

    function test_AbortMigration_EmitsEvent() public asOwner {
        address[] memory operators = getSortedOperatorArray(4);
        cluster.startMigration(operators, 0);
        
        vm.expectEmit(true, true, true, true);
        emit MigrationAborted(1, 2);
        cluster.abortMigration(1);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            SET MAINTENANCE
    //////////////////////////////////////////////////////////////////////////*/

    function test_RevertWhen_NonAuthorizedSetsMaintenance() public asAnyone {
        vm.expectRevert(Cluster.Unauthorized.selector);
        cluster.setMaintenance(true);
    }

    function test_SetMaintenance_OwnerCanSet() public asOwner {
        cluster.setMaintenance(true);
        // Owner's maintenance should not be stored in info mapping
        (address addr, bytes memory data, bool maintenance) = cluster.info(OWNER);
        assertEq(addr, address(0), "Owner info should be empty");
        assertEq(data, "", "Owner data should be empty");
        assertFalse(maintenance, "Owner maintenance not stored");
    }

    function test_SetMaintenance_OperatorCanSet() public {
        vm.startPrank(OPERATOR);
        cluster.setMaintenance(true);
        (,, bool maintenance) = cluster.info(OPERATOR);
        assertTrue(maintenance, "Operator maintenance should be set");
        vm.stopPrank();
    }

    function test_RevertWhen_SetMaintenanceOnUninitializedCluster() public {
        newCluster(0);
        vm.expectRevert(Cluster.NotInitialized.selector);
        vm.startPrank(OWNER);
        cluster.setMaintenance(true);
        vm.stopPrank();
    }

    function test_SetMaintenance_SingleOperatorMigration() public asOwner {
        address[] memory operators = getSortedOperatorArray(1);
        cluster.startMigration(operators, 0);
        
        (uint64 id, uint16 remaining, bool inProgress) = cluster.getMigrationStatus();
        assertEq(id, 1, "Migration ID");
        assertEq(remaining, 1, "Remaining operators");
        assertTrue(inProgress, "Migration in progress");
    }

    function test_SetMaintenance_BumpsVersion() public asOwner {
        cluster.setMaintenance(true);
        assertEq(cluster.version(), 1, "Version should increment");
    }

    function test_SetMaintenance_DoesNotBumpKeyspaceVersion() public asOwner {
        cluster.setMaintenance(true);
        assertEq(cluster.keyspaceVersion(), 0, "Keyspace version should not change");
    }

    function test_SetMaintenance_UpdatesFlag() public {
        vm.startPrank(OPERATOR);
        cluster.setMaintenance(true);
        (,, bool maintenance) = cluster.info(OPERATOR);
        assertTrue(maintenance, "Maintenance should be true");
        
        cluster.setMaintenance(false);
        (,, maintenance) = cluster.info(OPERATOR);
        assertFalse(maintenance, "Maintenance should be false");
        vm.stopPrank();
    }

    function test_SetMaintenance_EmitsEvent() public asOwner {
        vm.expectEmit(true, true, true, true);
        emit MaintenanceToggled(OWNER, true, 1);
        cluster.setMaintenance(true);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            UPDATE SETTINGS
    //////////////////////////////////////////////////////////////////////////*/

    function test_RevertWhen_NonOwnerUpdatesSettings() public asAnyone {
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, ANYONE));
        cluster.updateSettings(Settings({ maxOperatorDataBytes: 100 }));
    }

        function test_RevertWhen_OperatorUpdatesSettings() public asOperator {
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, OPERATOR));
        cluster.updateSettings(Settings({ maxOperatorDataBytes: 100 }));
    }

    function test_UpdateSettings_Success() public asOwner {
        cluster.updateSettings(Settings({ maxOperatorDataBytes: 100 }));
        uint16 maxOperatorDataBytes = cluster.settings();
        assertEq(maxOperatorDataBytes, 100, "Settings should be updated");
    }

    function test_UpdateSettings_EnforcesMaxDataBytes() public asOwner {
        cluster.updateSettings(Settings({ maxOperatorDataBytes: 5 }));
        
        uint256 operatorKey = 10;
        bytes memory tooLongData = "123456";
        vm.expectRevert(Cluster.InvalidOperatorData.selector);
        cluster.addNodeOperator(newNodeOperator(operatorKey, tooLongData));
    }

    function test_UpdateSettings_BumpsVersion() public asOwner {
        cluster.updateSettings(Settings({ maxOperatorDataBytes: 100 }));
        assertEq(cluster.version(), 1, "Version should increment");
    }

    function test_UpdateSettings_DoesNotBumpKeyspaceVersion() public asOwner {
        cluster.updateSettings(Settings({ maxOperatorDataBytes: 100 }));
        assertEq(cluster.keyspaceVersion(), 0, "Keyspace version should not change");
    }

    function test_UpdateSettings_EmitsEvent() public asOwner {
        Settings memory newSettings = Settings({ maxOperatorDataBytes: 100 });
        vm.expectEmit(true, true, true, true);
        emit SettingsUpdated(newSettings, OWNER, 1);
        cluster.updateSettings(newSettings);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            TRANSFER OWNERSHIP
    //////////////////////////////////////////////////////////////////////////*/

    function test_RevertWhen_NonOwnerTransfersOwnership() public asAnyone {
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, ANYONE));
        cluster.transferOwnership(ANYONE);
    }

    function test_RevertWhen_OperatorTransfersOwnership() public asOperator {
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, OPERATOR));
        cluster.transferOwnership(OPERATOR);
    }

    function test_TransferOwnership_Success() public asOwner {
        cluster.transferOwnership(NEW_OWNER);
        vm.stopPrank();
        vm.startPrank(NEW_OWNER);
        cluster.acceptOwnership();
        assertEq(cluster.owner(), NEW_OWNER, "Ownership should be transferred");
    }

    function test_TransferOwnership_RequiresAcceptance() public asOwner {
        cluster.transferOwnership(NEW_OWNER);
        // Ownership not transferred yet
        assertEq(cluster.owner(), OWNER, "Ownership should not change until accepted");
        
        vm.stopPrank();
        vm.startPrank(NEW_OWNER);
        cluster.acceptOwnership();
        // Now it's transferred
        assertEq(cluster.owner(), NEW_OWNER, "Ownership should be transferred after acceptance");
    }

    /*//////////////////////////////////////////////////////////////////////////
                            VIEW FUNCTIONS
    //////////////////////////////////////////////////////////////////////////*/

    function test_GetAllOperators() public {
        address[] memory operators = cluster.getAllOperators();
        assertEq(operators.length, 5, "Operator count");
        // Should be sorted
        for (uint256 i = 1; i < operators.length; i++) {
            assertTrue(operators[i-1] < operators[i], "Operators should be sorted");
        }
    }

    function test_GetAllOperators_EmptyCluster() public {
        newCluster(0);
        address[] memory operators = cluster.getAllOperators();
        assertEq(operators.length, 0, "Empty cluster should have no operators");
    }

    function test_GetOperatorAt() public {
        address firstOperator = cluster.getOperatorAt(0);
        assertTrue(cluster.isOperator(firstOperator), "Should return valid operator");
        
        address lastOperator = cluster.getOperatorAt(4);
        assertTrue(cluster.isOperator(lastOperator), "Should return valid operator");
    }

    function test_RevertWhen_GetOperatorAtInvalidIndex() public {
        vm.expectRevert(); // EnumerableSet will revert on invalid index
        cluster.getOperatorAt(999);
    }

    function test_GetPullingOperators_NoMigration() public {
        address[] memory pulling = cluster.getPullingOperators();
        assertEq(pulling.length, 0, "No pulling operators when no migration");
    }

    function test_GetPullingOperators_DuringMigration() public asOwner {
        address[] memory operators = getSortedOperatorArray(3);
        cluster.startMigration(operators, 0);
        
        address[] memory pulling = cluster.getPullingOperators();
        assertEq(pulling.length, 3, "All operators should be pulling");
        
        // Verify the operators are the expected ones
        for (uint256 i = 0; i < operators.length; i++) {
            bool found = false;
            for (uint256 j = 0; j < pulling.length; j++) {
                if (pulling[j] == operators[i]) {
                    found = true;
                    break;
                }
            }
            assertTrue(found, "Operator should be in pulling list");
        }
    }

    function test_GetPullingOperators_AfterPartialCompletion() public asOwner {
        address[] memory operators = getSortedOperatorArray(3);
        cluster.startMigration(operators, 0);
        vm.stopPrank();
        
        // Complete migration for one operator
        vm.startPrank(operators[0]);
        cluster.completeMigration(1);
        vm.stopPrank();
        
        address[] memory pulling = cluster.getPullingOperators();
        assertEq(pulling.length, 2, "Should have 2 operators still pulling");
        
        // Verify the completed operator is not in the list
        for (uint256 i = 0; i < pulling.length; i++) {
            assertTrue(pulling[i] != operators[0], "Completed operator should not be pulling");
        }
    }

    function test_GetMaintenanceOperators() public {
        vm.startPrank(vm.addr(1));
        cluster.setMaintenance(true);
        vm.stopPrank();
        
        vm.startPrank(vm.addr(3));
        cluster.setMaintenance(true);
        vm.stopPrank();
        
        address[] memory maintenanceOps = cluster.getMaintenanceOperators();
        assertEq(maintenanceOps.length, 2, "Maintenance operators count");
    }

    function test_GetMigrationStatus() public asOwner {
        (uint64 id, uint16 remaining, bool inProgress) = cluster.getMigrationStatus();
        assertEq(id, 0, "Initial migration ID");
        assertEq(remaining, 0, "Initial remaining count");
        assertFalse(inProgress, "No migration in progress initially");
        
        address[] memory operators = getSortedOperatorArray(2);
        cluster.startMigration(operators, 0);
        
        (id, remaining, inProgress) = cluster.getMigrationStatus();
        assertEq(id, 1, "Migration ID after start");
        assertEq(remaining, 2, "Remaining operators");
        assertTrue(inProgress, "Migration in progress");
    }

    /*//////////////////////////////////////////////////////////////////////////
                            HELPER FUNCTIONS
    //////////////////////////////////////////////////////////////////////////*/

    function newCluster() internal {   
        newCluster(5);
    }

    function newCluster(uint256 operatorsCount) internal {   
        newCluster(Settings({ maxOperatorDataBytes: MAX_OPERATOR_DATA_BYTES }), operatorsCount);
    }

    function newCluster(Settings memory settings, uint256 operatorsCount) internal {
        vm.startPrank(OWNER);

        NodeOperator[] memory operators = new NodeOperator[](operatorsCount);
        for (uint256 i = 0; i < operatorsCount; i++) {
            operators[i] = newNodeOperator(i + 1, DEFAULT_OPERATOR_DATA);
        }

        cluster = new Cluster(settings, operators);
        vm.stopPrank();
    }

    function getSortedOperatorArray(uint256 count) internal view returns (address[] memory) {
        require(count <= 5, "Only 5 operators available");
        address[] memory operators = new address[](count);
        for (uint256 i = 0; i < count; i++) {
            operators[i] = vm.addr(i + 1);
        }
        // Actually sort the addresses since vm.addr() doesn't guarantee order
        operators.sort();
        return operators;
    }


    function newNodeOperator(uint256 privateKey) internal pure returns (NodeOperator memory) {
        return newNodeOperator(privateKey, abi.encodePacked("operator ", privateKey.toString()));
    }

    function newNodeOperator(uint256 privateKey, bytes memory data) internal pure returns (NodeOperator memory) {
        return NodeOperator({ 
            addr: vm.addr(privateKey), 
            data: data,
            maintenance: false
        });
    }
} 