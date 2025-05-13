// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "../dependencies/forge-std-1.9.7/src/Test.sol";
import {Vm} from "../dependencies/forge-std-1.9.7/src/Vm.sol";
import "../dependencies/forge-std-1.9.7/src/console.sol";

import "../src/Cluster/Cluster.sol";

uint256 constant OWNER = 12345;
uint256 constant NEW_OWNER = 56789;
uint256 constant ANYONE = 9000;

uint256 constant OPERATOR_A = 1;
uint256 constant OPERATOR_B = 2;
uint256 constant OPERATOR_C = 3;
uint256 constant EXTRA_OPERATOR = 1000;

bytes constant DEFAULT_OPERATOR_DATA = "Some operator specific data";

uint256 constant NODE_A = 1;
uint256 constant NODE_B = 2;
uint256 constant EXTRA_NODE = 1000;

bytes constant DEFAULT_NODE_DATA = "Some node specific data";

uint256 constant MIN_OPERATORS = 3;
uint256 constant MIN_NODES = 1;

uint256 constant MAX_OPERATORS = 256;
uint256 constant MAX_NODES = 256;

contract ClusterTest is Test {
    Cluster cluster;
    ClusterView clusterView;

    constructor() {
        newCluster(vm);
    }

    // contructor

    function test_canNotCreateClusterWithTooFewOperators() public {
        expectRevert("too few operators");
        newCluster(vm, MIN_OPERATORS - 1, MIN_NODES);
    }

    function test_canNotCreateClusterWithTooFewNodes() public {
        expectRevert("too few nodes");
        newCluster(vm, MIN_OPERATORS, MIN_NODES - 1);
    }

    function test_canNotCreateClusterWithTooManyOperators() public {
        expectRevert("too many operators");
        newCluster(vm, MAX_OPERATORS + 1, MIN_NODES);
    }

    function test_canNotCreateClusterWithTooManyNodes() public {
        expectRevert("too many nodes");
        newCluster(vm, MIN_OPERATORS, MAX_NODES + 1);
    }

    function test_canCreateClusterWithMinNumberOfOperatorsAndNodes() public {
        newCluster(vm, MIN_OPERATORS, MIN_NODES);
    }

    function test_canCreateClusterWithMaxNumberOfOperators() public {
        newCluster(vm, MAX_OPERATORS, MIN_NODES);
    }

    function test_canCreateClusterWithMaxNumberOfNodes() public {
        newCluster(vm, MIN_OPERATORS, MAX_NODES);
    }

    function test_clusterContainsInitialOperators() public view {
        assertOperatorSlotsCount(MIN_OPERATORS);
        assertOperatorSlot(0, OPERATOR_A);
        assertOperatorSlot(1, OPERATOR_B);
        assertOperatorSlot(2, OPERATOR_C);
    }

    function test_clusterContainsInitialNodes() public {
        assertNodesCount(OPERATOR_A, MIN_NODES);
        assertNode(OPERATOR_A, NODE_A, DEFAULT_NODE_DATA);
        
        assertNodesCount(OPERATOR_B, MIN_NODES);
        assertNode(OPERATOR_B, NODE_A, DEFAULT_NODE_DATA);

        assertNodesCount(OPERATOR_C, MIN_NODES);
        assertNode(OPERATOR_C, NODE_A, DEFAULT_NODE_DATA);
    }

    function test_clusterInitialVersionIs0() public view {
        assertVersion(0);
    }

    function test_clusterInitialKeyspaceVersionIs0() public view {
        assertKeyspaceVersion(0);
    }

    // startMigration

    function test_anyoneCanNotStartMigration() public {
        expectRevert("not the owner");
        startMigration(ANYONE);
    }

    function test_operatorCanNotStartMigration() public {
        expectRevert("not the owner");
        startMigration(OPERATOR_A);
    }

    function test_ownerCanStartMigration() public {
        startMigration(OWNER);
    }

    function test_canNotStartMigrationWithTooManyOperators() public {
        expectRevert("too many operators");
        startMigration(OWNER, 0, MAX_OPERATORS - MIN_OPERATORS + 1);
    }

    function test_canNotStartMigrationWithTooFewOperators() public {
        expectRevert("too few operators");
        startMigration(OWNER, 1, 0);
    }

    function test_canStartMigrationWhenAddedOperatorsReplenishRemoved() public {
        startMigration(OWNER, MIN_OPERATORS, MIN_OPERATORS);
    }

    function test_canNotStartMigrationWithTooManyNodes() public {
        expectRevert("too many nodes");
        startMigration(OWNER, newMigration().addOperator(EXTRA_OPERATOR, MAX_NODES + 1));
    }

    function test_canNotStartMigrationWithTooFewNodes() public {
        expectRevert("too few nodes");
        startMigration(OWNER, newMigration().addOperator(EXTRA_OPERATOR, MIN_NODES - 1));
    }

    function test_canNotStartMigrationWhenMaintenanceInProgress() public {
        startMaintenance(OPERATOR_A);
        expectRevert("maintenance in progress");
        startMigration(OWNER, 0, 1);
    }

    function test_startMigrationBumpsVersion() public {
        startMigration(OWNER, 0, 1);
        assertVersion(1);
    }

    function test_startMigrationDoesNotBumpKeyspaceVersion() public {
        startMigration(OWNER, 0, 1);
        assertKeyspaceVersion(0);
    }

    function test_startMigrationEmitsMigrationStartedEvent() public {
        TestMigration memory migration = newMigration(2, 2);
        vm.expectEmit();
        emit MigrationStarted(migration.operatorsToRemove, migration.operatorsToAdd, 1);
        startMigration(OWNER, migration);
    }

    function test_startMigrationUpdatesMigration() public {
        newCluster(vm, 5, MIN_NODES);
        TestMigration memory migration = newMigration()
            .removeOperator(3)
            .removeOperator(2)
            .addOperator(6)
            .addOperator(7)
            .addOperator(8);

        startMigration(OWNER, migration);
        assertMigration(migration);
        assertMigrationPullingOperatorsCount(5 - 2 + 3);
        assertMigrationPullingOperator(0, 1);
        assertMigrationPullingOperator(1, 4);
        assertMigrationPullingOperator(2, 5);
        assertMigrationPullingOperator(3, 6);
        assertMigrationPullingOperator(4, 7);
        assertMigrationPullingOperator(5, 8);
    }

    // completeMigration

    function test_anyoneCanNotCompleteMigration() public {
        startMigration(OWNER, 0, 1);
        expectRevert("not pulling");
        completeMigration(ANYONE);
    }

    function test_ownerCanNotCompleteMigration() public {
        startMigration(OWNER, 0, 1);
        expectRevert("not pulling");
        completeMigration(OWNER);
    }

    function test_operatorCanNotCompleteNonExistentMigration() public {
        expectRevert("not pulling");
        completeMigration(OPERATOR_A);
    }

    function test_operatorCanCompleteMigration() public {
        startMigration(OWNER, 0, 1);
        completeMigration(OPERATOR_A);
    }

    function test_operatorCanNotCompleteMigrationTwice() public {
        startMigration(OWNER, 0, 1);
        completeMigration(OPERATOR_A);
        expectRevert("not pulling");
        completeMigration(OPERATOR_A);
    }

    function test_completeMigrationBumpsVersion() public {
        startMigration(OWNER, 0, 1);
        completeMigration(OPERATOR_A);
        assertVersion(2);
    }

    function test_completeMigrationBumpsVersionForEachCompletedPullAndForCompletionItself() public {
        startMigration(OWNER, 0, 1);
        completeMigration(OPERATOR_A);
        completeMigration(OPERATOR_B);
        completeMigration(OPERATOR_C);
        completeMigration(EXTRA_OPERATOR);
        assertVersion(6); 
    }

    function test_completeMigrationDoesNotUpdateOperatorsIfNotCompleted() public {
        startMigration(OWNER, 0, 1);
        completeMigration(OPERATOR_A);
        assertOperatorSlotsCount(MIN_OPERATORS);
        assertOperatorSlot(0, OPERATOR_A);
        assertOperatorSlot(1, OPERATOR_B);
        assertOperatorSlot(2, OPERATOR_C);
    }

    function test_completeMigrationUpdatesOperatorsIfCompleted() public {
        newCluster(vm, 5, MIN_NODES);
        TestMigration memory migration = newMigration()
            .removeOperator(3)
            .removeOperator(4)
            .removeOperator(2)
            .addOperator(6)
            .addOperator(7);

        startMigration(OWNER, migration);
        completeMigration(1);
        completeMigration(5);
        completeMigration(6);
        completeMigration(7);

        assertOperatorSlotsCount(5);
        assertOperatorSlot(0, 1);
        assertOperatorSlot(1, 6);
        assertOperatorSlotEmpty(2);
        assertOperatorSlot(3, 7);
        assertOperatorSlot(4, 5);
    }

    function test_completeMigrationDeletesMigrationIfCompleted() public {
        startMigration(OWNER, 0, 1);
        completeMigration(OPERATOR_A);
        completeMigration(OPERATOR_B);
        completeMigration(OPERATOR_C);
        completeMigration(EXTRA_OPERATOR);
        assertNoMigration();
    }

    function test_completeMigrationUpdatesMigrationIfNotCompleted() public {
        startMigration(OWNER, 0, 1);
        completeMigration(OPERATOR_A);
        assertMigrationPullingOperatorsCount(MIN_OPERATORS + 1 - 1);
        assertMigrationPullingOperator(0, OPERATOR_B);
        assertMigrationPullingOperator(1, OPERATOR_C);
        assertMigrationPullingOperator(2, EXTRA_OPERATOR);
    }

    function test_completeMigrationDoesNotBumpKeyspaceVersionIfNotCompleted() public {
        startMigration(OWNER, 0, 1);
        completeMigration(OPERATOR_A);
        assertKeyspaceVersion(0);
    }

    function test_completeMigrationBumpKeyspaceVersionIfCompleted() public {
        startMigration(OWNER, 0, 1);
        completeMigration(OPERATOR_A);
        completeMigration(OPERATOR_B);
        completeMigration(OPERATOR_C);
        completeMigration(EXTRA_OPERATOR);
        assertKeyspaceVersion(1);
    }

    function test_completeMigrationEmitsMigrationDataPullCompletedEventIfNotCompleted() public {
        startMigration(OWNER, 0, 1);
        vm.expectEmit();
        emit MigrationDataPullCompleted(vm.addr(OPERATOR_A), 2);
        completeMigration(OPERATOR_A);
    }

    function test_completeMigrationEmitsMigrationCompletedEventIfCompleted() public {
        startMigration(OWNER, 0, 1);
        completeMigration(OPERATOR_A);
        completeMigration(OPERATOR_B);
        completeMigration(OPERATOR_C);
        vm.expectEmit();
        emit MigrationDataPullCompleted(vm.addr(EXTRA_OPERATOR), 5);
        emit MigrationCompleted(6);
        completeMigration(EXTRA_OPERATOR);
    }

    // abortMigration

    function test_anyoneCanNotAbortMigration() public {
        startMigration(OWNER, 0, 1);
        expectRevert("not the owner");
        abortMigration(ANYONE);
    }

    function test_operatorCanNotAbortMigration() public {
        startMigration(OWNER, 0, 1);
        expectRevert("not the owner");
        abortMigration(OPERATOR_A);
    }

    function test_ownerCanAbortMigration() public {
        startMigration(OWNER, 0, 1);
        abortMigration(OWNER);
    }

    function test_canNotAbortNonExistentMigration() public {
        expectRevert("not in progress");
        abortMigration(OWNER);
    }

    function test_abortMigrationBumpsVersion() public {
        startMigration(OWNER, 0, 1);
        abortMigration(OWNER);
        assertVersion(2);
    }

    function test_abortMigrationDoesNotBumpKeyspaceVersion() public {
        startMigration(OWNER, 0, 1);
        abortMigration(OWNER);
        assertKeyspaceVersion(0);
    }

    function test_abortMigrationDoesNotUpdateOperators() public {
        startMigration(OWNER, 0, 1);
        abortMigration(OWNER);
        assertOperatorSlotsCount(MIN_OPERATORS);
        assertOperatorSlot(0, OPERATOR_A);
        assertOperatorSlot(1, OPERATOR_B);
        assertOperatorSlot(2, OPERATOR_C);
    }

    function test_abortMigrationDeletesMigration() public {
        startMigration(OWNER, 0, 1);
        abortMigration(OWNER);
        assertNoMigration();
    }

    function test_abortMigrationEmitsMigrationAbortedEvent() public {
        startMigration(OWNER, 0, 1);
        vm.expectEmit();
        emit MigrationAborted(2);
        abortMigration(OWNER);
    }

    // startMaintenance 

    function test_anyoneCanNotStartMaintenance() public {
        expectRevert("not an operator");
        startMaintenance(ANYONE);
    }

    function test_ownerCanNotStartMaintenance() public {
        expectRevert("not an operator");
        startMaintenance(OWNER);
    }

    function test_operatorCanStartMaintenance() public {
        startMaintenance(OPERATOR_A);
    }

    function test_canNotStartMoreThanOneMaintenance() public {
        startMaintenance(OPERATOR_A);
        expectRevert("another maintenance in progress");
        startMaintenance(OPERATOR_B);
    }

    function test_canNotStartMaintenanceTwice() public {
        startMaintenance(OPERATOR_A);
        expectRevert("another maintenance in progress");
        startMaintenance(OPERATOR_A);
    }

    function test_operatorCanNotStartMaintenanceWhenMigrationInProgress() public {
        startMigration(OWNER, 0, 1);
        expectRevert("migration in progress");
        startMaintenance(OPERATOR_A);
    }

    function test_startMaintenanceBumpsVersion() public {
        startMaintenance(OPERATOR_A);
        assertVersion(1);
    }

    function test_startMaintenanceDoesNotBumpKeyspaceVersion() public {
        startMaintenance(OPERATOR_A);
        assertKeyspaceVersion(0);
    }

    function test_startMaintenanceUpdatesMaintenance() public {
        startMaintenance(OPERATOR_A);
        assertMaintenance(OPERATOR_A);
    }

    function test_startMaintenanceEmitsMaintenanceStartedEvent() public {
        vm.expectEmit();
        emit MaintenanceStarted(vm.addr(OPERATOR_A), 1);
        startMaintenance(OPERATOR_A);
    }

    // completeMaintenance

    function test_anyoneCanNotCompleteMaintenance() public {
        expectRevert("not an operator");
        startMaintenance(ANYONE);
    }

    function test_anotherOperatorCanNotCompleteMaintenance() public {
        startMaintenance(OPERATOR_A);
        expectRevert("not under maintenance");
        completeMaintenance(OPERATOR_B);
    }

    function test_ownerCanNotCompleteMaintenance() public {
        startMaintenance(OPERATOR_A);
        expectRevert("not an operator");
        completeMaintenance(OWNER);
    }

    function test_sameOperatorCanCompleteMaintenance() public {
        startMaintenance(OPERATOR_A);
        completeMaintenance(OPERATOR_A);
    }

    function test_canNotCompleteNonExistentMaintenance() public {
        expectRevert("not under maintenance");
        completeMaintenance(OPERATOR_A);
    }

    function test_completeMaintenanceBumpsVersion() public {
        startMaintenance(OPERATOR_A);
        completeMaintenance(OPERATOR_A);
        assertVersion(2);
    }

    function test_completeMaintenanceDoesNotBumpKeyspaceVersion() public {
        startMaintenance(OPERATOR_A);
        completeMaintenance(OPERATOR_A);
        assertKeyspaceVersion(0);
    }

    function test_completeMaintenanceDeletesMaintenance() public {
        startMaintenance(OPERATOR_A);
        completeMaintenance(OPERATOR_A);
        assertNoMaintenance();
    }
    
    function test_completeMaintenanceEmitsMaintenanceCompletedEvent() public {
        startMaintenance(OPERATOR_A);
        vm.expectEmit();
        emit MaintenanceCompleted(vm.addr(OPERATOR_A), 2);
        completeMaintenance(OPERATOR_A);
    }

    // abortMaintenance

    function test_anyoneCanNotAbortMaintenance() public {
        startMaintenance(OPERATOR_A);
        expectRevert("not the owner");
        abortMaintenance(ANYONE);
    }

    function test_operatorCanNotAbortMaintenance() public {
        startMaintenance(OPERATOR_A);
        expectRevert("not the owner");
        abortMaintenance(OPERATOR_B);
    }

    function test_ownerCanAbortMaintenance() public {
        startMaintenance(OPERATOR_A);
        abortMaintenance(OWNER);
    }

    function test_canNotAbortNonExistentMaintenance() public {
        expectRevert("not under maintenance");
        abortMaintenance(OWNER);
    }

    function test_abortMaintenanceBumpsVersion() public {
        startMaintenance(OPERATOR_A);
        abortMaintenance(OWNER);
        assertVersion(2);
    }

    function test_abortMaintenanceDoesNotBumpKeyspaceVersion() public {
        startMaintenance(OPERATOR_A);
        abortMaintenance(OWNER);
        assertKeyspaceVersion(0);
    }

    function test_abortMaintenanceDeletesMaintenance() public {
        startMaintenance(OPERATOR_A);
        abortMaintenance(OWNER);
        assertNoMaintenance();
    }

    function test_abortMaintenanceEmitsMaintenanceAbortedEvent() public {
        startMaintenance(OPERATOR_A);
        vm.expectEmit();
        emit MaintenanceAborted(2);
        abortMaintenance(OWNER);
    }

    // setNode
    
    function test_anyoneCanNotSetNode() public {
        expectRevert("not an operator");
        setNode(ANYONE, NODE_A, "data");
    }

    function test_ownerCanNotSetNode() public {
        expectRevert("not an operator");
        setNode(OWNER, NODE_A, "data");
    }

    function test_operatorCanSetNode() public {
        setNode(OPERATOR_A, NODE_A, "data");
    }

    function test_canSetSameNode() public {
        setNode(OPERATOR_A, NODE_A, "data");
        setNode(OPERATOR_A, NODE_A, "data");
    }

    function test_canNotSetNewNodeWhenTooManyNodes() public {
        newCluster(vm, MIN_OPERATORS, MAX_NODES);
        expectRevert("too many nodes");
        setNode(OPERATOR_A, EXTRA_NODE, "data");
    }

    function test_setNodeWithNewIdDoesNotOverwriteExistingOnes() public {
        assertNodesCount(OPERATOR_A, 1);
        assertNode(OPERATOR_A, NODE_A, DEFAULT_NODE_DATA);
        setNode(OPERATOR_A, EXTRA_NODE, "data");
        assertNodesCount(OPERATOR_A, 2);
        assertNode(OPERATOR_A, EXTRA_NODE, "data");
    }

    function test_setNodeWithSameIdOverwritesExistingOne() public {
        setNode(OPERATOR_A, NODE_A, "data");
        assertNodesCount(OPERATOR_A, 1);
        assertNode(OPERATOR_A, NODE_A, "data");
    }

    function test_setNodeBumpsVersion() public {
        setNode(OPERATOR_A, NODE_A, "data");
        assertVersion(1);   
    }

    function test_setNodeDoesNotBumpKeyspaceVersion() public {
        setNode(OPERATOR_A, NODE_A, "data");
        assertKeyspaceVersion(0);
    }

    function test_setNodeEmitsNodeSetEvent() public {
        vm.expectEmit();
        emit NodeSet(vm.addr(OPERATOR_A), Node({ id: NODE_A, data: "NodeSet data"}), 1);
        setNode(OPERATOR_A, NODE_A, "NodeSet data");
    }

    // removeNode

    function test_anyoneCanNotRemoveNode() public {
        expectRevert("not an operator");
        removeNode(ANYONE, NODE_A);
    }

    function test_ownerCanNotRemoveNode() public {
        expectRevert("not an operator");
        removeNode(OWNER, NODE_A);
    }

    function test_operatorCanRemoveNode() public {
        setNode(OPERATOR_A, EXTRA_NODE, "data");
        removeNode(OPERATOR_A, NODE_A);
    }

    function test_canNotRemoveNonExistentNode() public {
        expectRevert("node doesn't exist");
        removeNode(OPERATOR_A, EXTRA_NODE);
    }

    function test_canNotRemoveNodeWhenTooFewNodes() public {
        expectRevert("too few nodes");
        removeNode(OPERATOR_A, NODE_A);
    }

    function test_removeNodeBumpsVersion() public {
        setNode(OPERATOR_A, EXTRA_NODE, "data");
        removeNode(OPERATOR_A, NODE_A);
        assertVersion(2);
    }

    function test_removeNodeDoesRemoveTheNode() public {
        setNode(OPERATOR_A, EXTRA_NODE, "data");
        removeNode(OPERATOR_A, NODE_A);
        assertNodesCount(OPERATOR_A, 1);
        assertNode(OPERATOR_A, EXTRA_NODE, "data");
    }

    function test_removeNodeDoesNotBumpKeyspaceVersion() public {
        setNode(OPERATOR_A, EXTRA_NODE, "data");
        removeNode(OPERATOR_A, NODE_A);
        assertKeyspaceVersion(0);
    }

    function test_removeNodeEmitsNodeRemovedEvent() public {
        setNode(OPERATOR_A, EXTRA_NODE, "data");
        vm.expectEmit();
        emit NodeRemoved(vm.addr(OPERATOR_A), NODE_A, 2);
        removeNode(OPERATOR_A, NODE_A);
    }

    // updateSettings

    function test_anyoneCanNotUpdateSettings() public {
        expectRevert("not the owner");
        updateSettings(ANYONE, Settings({ minOperators: 5, minNodes: 2 }));
    }

    function test_operatorCanNotUpdateSettings() public {
        expectRevert("not the owner");
        updateSettings(OPERATOR_A, Settings({ minOperators: 5, minNodes: 2 }));
    }

    function test_ownerCanUpdateSettings() public {
        updateSettings(OWNER, Settings({ minOperators: 5, minNodes: 2 }));
    }

    function test_updateSettingsCorrectlyUpdatesMinOperators() public {
        newCluster(vm, 5, MIN_NODES);
        updateSettings(OWNER, Settings({ minOperators: 3, minNodes: uint8(MIN_NODES) }));
        startMigration(OWNER, 1, 0);
    }

    function test_updateSettingsCorrectlyUpdatesMinNodes() public {
        newCluster(vm, MIN_OPERATORS, 2);
        updateSettings(OWNER, Settings({ minOperators: uint8(MIN_OPERATORS), minNodes: 1 }));
        removeNode(OPERATOR_A, NODE_A);
    }

    // transferOwnership

    function test_anyoneCanNotTransferOwnership() public {
        expectRevert("not the owner");
        transferOwnership(ANYONE, ANYONE);
    }

    function test_operatorCanNotTransferOwnership() public {
        expectRevert("not the owner");
        transferOwnership(OPERATOR_A, OPERATOR_A);
    }

    function test_ownerCanTransferOwnership() public {
        transferOwnership(OWNER, NEW_OWNER);
    }

    function test_transferOwnershipChangesOwner() public {
        transferOwnership(OWNER, NEW_OWNER);
        startMigration(NEW_OWNER, 0, 1);
    }

    // full lifecycle

    function test_fullClusterLifecycle() public {
        newCluster(vm, Settings({ minOperators: 5, minNodes: 1 }), 9, 1);
        setNode(OPERATOR_A, EXTRA_NODE, "A");
        startMaintenance(OPERATOR_B);
        setNode(OPERATOR_C, EXTRA_NODE, "C");
        completeMaintenance(OPERATOR_B);
        startMigration(OWNER, newMigration().addOperator(10).addOperator(11).addOperator(12));
        removeNode(OPERATOR_A, NODE_A);
        for (uint256 i = 1; i <= 12; i++) {
            completeMigration(i);
        }
        removeNode(OPERATOR_C, NODE_A);
        startMaintenance(11);
        setNode(11, NODE_A, "11");
        abortMaintenance(OWNER);

        startMigration(OWNER, newMigration().removeOperator(11));
        for (uint256 i = 1; i <= 12; i++) {
            if (i != 11) {
                completeMigration(i);
            }
        }

        startMigration(OWNER, newMigration().addOperator(13));
        for (uint256 i = 1; i <= 12; i++) {
            if (i != 11) {
                completeMigration(i);
            }
        }
        abortMigration(OWNER);

        transferOwnership(OWNER, NEW_OWNER);
        updateSettings(NEW_OWNER, Settings({ minOperators: 7, minNodes: 1 }));

        assertKeyspaceVersion(2);
    }

    // internal

    function newCluster(Vm vm) internal {   
        newCluster(vm, Settings({ minOperators: uint8(MIN_OPERATORS), minNodes: uint8(MIN_NODES) }));
    }

    function newCluster(Vm vm, Settings memory settings) internal {   
        newCluster(vm, settings, settings.minOperators, settings.minNodes);
    }

    function newCluster(Vm vm, uint256 operatorsCount, uint256 nodesCount) internal {   
        newCluster(vm, Settings({ minOperators: uint8(MIN_OPERATORS), minNodes: uint8(MIN_NODES) }), operatorsCount, nodesCount);
    }

    function newCluster(Vm vm, Settings memory settings, uint256 operatorsCount, uint256 nodesCount) internal {
        setCaller(OWNER);

        NodeOperatorView[] memory operators = new NodeOperatorView[](operatorsCount);
        for (uint256 i = 0; i < operatorsCount; i++) {
            operators[i] = newNodeOperator(vm.addr(i + 1), nodesCount);
        }

        cluster = new Cluster(settings, operators);
        // ECRecover address. Constructor failed
        if (address(cluster) == address(1)) {
            return;
        }

        updateClusterView();
    }

    function updateClusterView() internal {
        clusterView = cluster.getView();
    }

    function setCaller(uint256 caller) internal {
        vm.prank(vm.addr(caller));
    }

    function expectRevert(bytes memory revertBytes) internal {
        vm.expectRevert(revertBytes);
    }

    function assertVersion(uint128 expectedVersion) internal view {
        assertEq(cluster.getView().version, expectedVersion);
    }

    function assertKeyspaceVersion(uint64 expectedVersion) internal view {
        assertEq(cluster.getView().keyspaceVersion, expectedVersion);
    }

    function assertOperatorSlotsCount(uint256 count) internal view {
        assertEq(clusterView.operators.slots.length, count);
    }

    function assertOperatorSlot(uint256 index, uint256 privateKey) internal view {
        assertEq(clusterView.operators.slots[index].addr, vm.addr(privateKey));
    }

    function assertOperatorSlotEmpty(uint256 index) internal view {
        assertEq(clusterView.operators.slots[index].addr, address(0));
    }

    function assertNodesCount(uint256 operator, uint256 count) internal {
        address addr = vm.addr(operator);
    
        for (uint256 i = 0; i < clusterView.operators.slots.length; i++) {
            if (clusterView.operators.slots[i].addr == addr) {
                assertEq(clusterView.operators.slots[i].nodes.length, count);
                return;
            }
        }

        fail();
    }

    function assertNode(uint256 operator, uint256 id, bytes memory data) internal {
        address addr = vm.addr(operator);

        for (uint256 i = 0; i < clusterView.operators.slots.length; i++) {
            if (clusterView.operators.slots[i].addr == addr) {
                for (uint256 j = 0; j < clusterView.operators.slots[i].nodes.length; j++) {
                    if (clusterView.operators.slots[i].nodes[j].id == id) {
                        assertEq(clusterView.operators.slots[i].nodes[j].data, data);
                        return;
                    }
                }
                fail();
            }
        }

        fail();
    }

    function assertMigration(TestMigration memory migration) internal view {
        assertEq(clusterView.migration.operatorsToRemove.length, migration.operatorsToRemove.length);
        for (uint256 i = 0; i < clusterView.migration.operatorsToRemove.length; i++) {
            assertEq(clusterView.migration.operatorsToRemove[i], migration.operatorsToRemove[i]);
        }

        assertEq(clusterView.migration.operatorsToAdd.length, migration.operatorsToAdd.length);
        for (uint256 i = 0; i < clusterView.migration.operatorsToAdd.length; i++) {
            assertEq(clusterView.migration.operatorsToAdd[i].addr, migration.operatorsToAdd[i].addr);
            assertEq(clusterView.migration.operatorsToAdd[i].data, migration.operatorsToAdd[i].data);
            for (uint256 j = 0; j < clusterView.migration.operatorsToAdd[i].nodes.length; j++) {
                assertEq(clusterView.migration.operatorsToAdd[i].nodes[j].id, migration.operatorsToAdd[i].nodes[j].id);
                assertEq(clusterView.migration.operatorsToAdd[i].nodes[j].data, migration.operatorsToAdd[i].nodes[j].data);
            }
        }
    }

    function assertNoMigration() internal view {
        TestMigration memory migration;
        assertMigration(migration);
    }

    function assertMigrationPullingOperatorsCount(uint256 count) internal view {
        assertEq(clusterView.migration.pullingOperators.length, count);
    }

    function assertMigrationPullingOperator(uint256 idx, uint256 operator) internal view {
        assertEq(clusterView.migration.pullingOperators[idx], vm.addr(operator));
    }

    function assertMaintenance(uint256 operator) internal view {
        assertEq(clusterView.maintenance.slot, vm.addr(operator));
    }

    function assertNoMaintenance() internal view {
        assertEq(clusterView.maintenance.slot, address(0));
    }

    // function assertMigrationOperatorsToRemoveCount(uint256 count) internal {
    //     assertEq(clusterView.migration.operatorsToRemove.length, count);
    // }

    // function assertMigrationOperatorsToAddCount(uint256 count) internal {
    //     assertEq(clusterView.migration.operatorsToAdd.length, count);
    // }

    // function assertMigrationOperatorToAdd(uint256 idx, uint256 privateKey, bytes: data, uint256 nodesCount) internal {
    //     assertEq(clusterView.migration.operatorsToAdd[idx].addr, vm.addr(privateKey));
    //     assertEq(clusterView.migration.operatorsToAdd[idx].data, data);
    //     assertEq(clusterView.migration.operatorsToAdd[idx].nodes.length, nodesCount);
    // }

    function newMigration() internal pure returns (TestMigration memory) {
        return TestMigration({
            vm: vm,
            operatorsToRemove: new address[](0),
            operatorsToAdd: new NodeOperatorView[](0)
        });
    } 

    function newMigration(uint256 toRemove, uint256 toAdd) internal view returns (TestMigration memory) {
        TestMigration memory migration = TestMigration({
            vm: vm,
            operatorsToRemove: new address[](toRemove),
            operatorsToAdd: new NodeOperatorView[](toAdd)
        });

        uint256 j;
        for (uint256 i = 0; i < clusterView.operators.slots.length; i++) {
            if (toRemove == 0) {
                break;
            }

            if (clusterView.operators.slots[i].addr != address(0)) {
                migration.operatorsToRemove[j] = clusterView.operators.slots[i].addr;
                j++;
                toRemove--;
            }
        }
        require(toRemove == 0);

        for (uint256 i = 0; i < toAdd; i++) {
            migration.operatorsToAdd[i] = newNodeOperator(vm.addr(EXTRA_OPERATOR + i), MIN_NODES);
        }

        return migration;
    }

    function startMigration(uint256 caller) internal {
        startMigration(caller, 0, 1);
    }

    function startMigration(uint256 caller, uint256 toRemove, uint256 toAdd) internal {
        startMigration(caller, newMigration(toRemove, toAdd));
    }

    function startMigration(uint256 caller, TestMigration memory migration) internal {
        setCaller(caller);
        cluster.startMigration(migration.operatorsToRemove, migration.operatorsToAdd);
        updateClusterView();
    }

    function completeMigration(uint256 caller) internal {
        setCaller(caller);
        cluster.completeMigration();
        updateClusterView();
    }

    function abortMigration(uint256 caller) internal {
        setCaller(caller);
        cluster.abortMigration();
        updateClusterView();
    }

    function startMaintenance(uint256 caller) internal {
        setCaller(caller);
        cluster.startMaintenance();
        updateClusterView();
    }

    function completeMaintenance(uint256 caller) internal {
        setCaller(caller);
        cluster.completeMaintenance();
        updateClusterView();
    }

    function abortMaintenance(uint256 caller) internal {
        setCaller(caller);
        cluster.abortMaintenance();
        updateClusterView();
    }

    function setNode(uint256 caller, uint256 id, bytes memory data) internal {
        setCaller(caller);
        cluster.setNode(Node({ id: id, data: data }));
        updateClusterView();
    }

    function removeNode(uint256 caller, uint256 id) internal {
        setCaller(caller);
        cluster.removeNode(id);
        updateClusterView();
    }

    function updateSettings(uint256 caller, Settings memory settings) internal {
        setCaller(caller);
        cluster.updateSettings(settings);
    }

    function transferOwnership(uint256 caller, uint256 newOwner) internal {
        setCaller(caller);
        cluster.transferOwnership(vm.addr(newOwner));
    }
}

struct TestMigration {
    Vm vm;

    address[] operatorsToRemove;
    NodeOperatorView[] operatorsToAdd;
}

library TestMigrationLib {
    function addOperator(TestMigration memory self, uint256 privateKey) internal pure returns (TestMigration memory) {
        return addOperator(self, privateKey, MIN_NODES);
    }

    function addOperator(TestMigration memory self, uint256 privateKey, uint256 nodesCount) internal pure returns (TestMigration memory) {
        NodeOperatorView[] memory operators = new NodeOperatorView[](self.operatorsToAdd.length + 1);
        for (uint256 i = 0; i < self.operatorsToAdd.length; i++) {
            operators[i] = self.operatorsToAdd[i];
        }
        operators[operators.length - 1] = newNodeOperator(self.vm.addr(privateKey), nodesCount);

        self.operatorsToAdd = operators;
        return self;
    }

    function removeOperator(TestMigration memory self, uint256 privateKey) internal pure returns (TestMigration memory) {
        address[] memory operators = new address[](self.operatorsToRemove.length + 1);
        for (uint256 i = 0; i < self.operatorsToRemove.length; i++) {
            operators[i] = self.operatorsToRemove[i];
        }
        operators[operators.length - 1] = self.vm.addr(privateKey); 

        self.operatorsToRemove = operators;
        return self;
    }
}

using TestMigrationLib for TestMigration;

function newNodeOperator(address addr, uint256 nodesCount) pure returns (NodeOperatorView memory) {
    Node[] memory nodes = new Node[](nodesCount);
    for (uint256 i = 0; i < nodesCount; i++) {
        nodes[i] = Node({ id: i + 1 , data: DEFAULT_NODE_DATA });
    }

    return NodeOperatorView({
        addr: addr,
        nodes: nodes,
        data: DEFAULT_OPERATOR_DATA
    });
}
