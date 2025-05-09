// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "../dependencies/forge-std-1.9.7/src/Test.sol";
import {Vm} from "../dependencies/forge-std-1.9.7/src/Vm.sol";
import "../dependencies/forge-std-1.9.7/src/console.sol";

import "../src/Cluster/Cluster.sol";

uint256 constant OWNER = 12345;
uint256 constant ANYONE = 9000;

uint256 constant OPERATOR_A = 1;
uint256 constant OPERATOR_B = 2;
uint256 constant OPERATOR_C = 3;

uint256 constant EXTRA_OPERATOR = 1000;

uint256 constant MIN_OPERATORS = 5;
uint256 constant MIN_NODES = 1;

uint256 constant MAX_OPERATORS = 256;
uint256 constant MAX_NODES = 256;

contract ClusterTest is Test {
    Cluster cluster;
    ClusterView clusterView;

    mapping(address => uint256) privateKeys;

    constructor() {
        newCluster(vm);
        updateClusterView();
    
        for (uint256 k = 1; k <= 10; k++) {
            privateKeys[vm.addr(k)] = k;
        }
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

    function test_clusterInitialVersionIs0() public {
        assertVersion(0);
    }

    function test_clusterInitialKeyspaceVersionIs0() public {
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

    function test_completeMigrationDoesNotBumpKeyspaceVersionIfNotCompleted() public {
        startMigration(OWNER, 0, 1);
        completeMigration(OPERATOR_A);
        assertKeyspaceVersion(0);
    }

    function test_completeMigrationBumpKeyspaceVersionIfCompleted() public {
        newCluster(vm, Settings({ minOperators: 3, minNodes: uint8(MIN_NODES) }));
        startMigration(OWNER, 0, 1);
        completeMigration(OPERATOR_A);
        completeMigration(OPERATOR_B);
        completeMigration(OPERATOR_C);
        completeMigration(EXTRA_OPERATOR);
        assertKeyspaceVersion(1);
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

    function test_operatorCanNotStartMaintenanceWhenMigrationInProgress() public {
        startMigration(OWNER, 0, 1);
        expectRevert("migration in progress");
        startMaintenance(OPERATOR_A);
    }

    // copleteMaintenance

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

    function newCluster(Vm vm) internal {   
        return newCluster(vm, Settings({ minOperators: uint8(MIN_OPERATORS), minNodes: uint8(MIN_NODES) }));
    }

    function newCluster(Vm vm, Settings memory settings) internal {   
        return newCluster(vm, settings, settings.minOperators, settings.minNodes);
    }

    function newCluster(Vm vm, uint256 operatorsCount, uint256 nodesCount) internal {   
        return newCluster(vm, Settings({ minOperators: uint8(MIN_OPERATORS), minNodes: uint8(MIN_NODES) }), operatorsCount, nodesCount);
    }

    function newCluster(Vm vm, Settings memory settings, uint256 operatorsCount, uint256 nodesCount) internal {
        setCaller(OWNER);

        NodeOperatorView[] memory operators = new NodeOperatorView[](operatorsCount);
        for (uint256 i = 0; i < operatorsCount; i++) {
            operators[i] = newNodeOperator(vm.addr(i + 1), nodesCount);
        }
        
        cluster = new Cluster(settings, operators);
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

    function assertVersion(uint128 expectedVersion) internal {
        assertEq(cluster.getView().version, expectedVersion);
    }

    function assertKeyspaceVersion(uint64 expectedVersion) internal {
        assertEq(cluster.getView().keyspaceVersion, expectedVersion);
    }

    function newMigration() internal returns (TestMigration memory) {
        return TestMigration({
            vm: vm,
            operatorsToRemove: new address[](0),
            operatorsToAdd: new NodeOperatorView[](0)
        });
    } 

    function newMigration(uint256 toRemove, uint256 toAdd) internal returns (TestMigration memory) {
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
            migration.operatorsToAdd[i] = newNodeOperator(vm.addr(EXTRA_OPERATOR + i), 2);
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
}

struct TestMigration {
    Vm vm;

    address[] operatorsToRemove;
    NodeOperatorView[] operatorsToAdd;
}

library TestMigrationLib {
    function addOperator(TestMigration memory self, uint256 privateKey) internal returns (TestMigration memory) {
        return addOperator(self, privateKey, 2);
    }

    function addOperator(TestMigration memory self, uint256 privateKey, uint256 nodesCount) internal returns (TestMigration memory) {
        Node[] memory nodes = new Node[](nodesCount);
        for (uint256 i = 0; i < nodesCount; i++) {
            nodes[i] = Node({ id: i + 1 , data: bytes("Some node specific data") });
        }

        NodeOperatorView[] memory operators = new NodeOperatorView[](self.operatorsToAdd.length + 1);
        for (uint256 i = 0; i < self.operatorsToAdd.length; i++) {
            operators[i] = self.operatorsToAdd[i];
        }
        operators[operators.length - 1] = NodeOperatorView({
            addr: self.vm.addr(privateKey),
            nodes: nodes,
            data: bytes("Some operator specific data")
        });

        self.operatorsToAdd = operators;
        return self;
    }

    function removeOperator(TestMigration memory self, uint256 privateKey) internal returns (TestMigration memory) {
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
        nodes[i] = Node({ id: i + 1 , data: bytes("Some node specific data") });
    }

    return NodeOperatorView({
        addr: addr,
        nodes: nodes,
        data: bytes("Some operator specific data")
    });
}

