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

contract ClusterTestSuite is Test {
    using TestClusterLib for TestCluster;

    mapping(address => uint256) privateKeys;

    constructor() {
        for (uint256 k = 1; k <= 10; k++) {
            privateKeys[vm.addr(k)] = k;
        }
    }

    // contructor

    function test_canNotCreateClusterWithTooFewOperators() public {
        vm.expectRevert("too few operators");
        newTestCluster(vm, MIN_OPERATORS - 1, MIN_NODES);
    }

    function test_canNotCreateClusterWithTooFewNodes() public {
        vm.expectRevert("too few nodes");
        newTestCluster(vm, MIN_OPERATORS, MIN_NODES - 1);
    }

    function test_canNotCreateClusterWithTooManyOperators() public {
        vm.expectRevert("too many operators");
        newTestCluster(vm, MAX_OPERATORS + 1, MIN_NODES);
    }

    function test_canNotCreateClusterWithTooManyNodes() public {
        vm.expectRevert("too many nodes");
        newTestCluster(vm, MIN_OPERATORS, MAX_NODES + 1);
    }

    function test_canCreateClusterWithMinNumberOfOperatorsAndNodes() public {
        newTestCluster(vm, MIN_OPERATORS, MIN_NODES);
    }

    function test_canCreateClusterWithMaxNumberOfOperators() public {
        newTestCluster(vm, MAX_OPERATORS, MIN_NODES);
    }

    function test_canCreateClusterWithMaxNumberOfNodes() public {
        newTestCluster(vm, MIN_OPERATORS, MAX_NODES);
    }

    function test_clusterInitialVersionIs0() public {
        newTestCluster(vm).assertVersion(0);
    }

    function test_clusterInitialKeyspaceVersionIs0() public {
        newTestCluster(vm).assertKeyspaceVersion(0);
    }

    // startMigration

    function test_anyoneCanNotStartMigration() public {
        newTestCluster(vm)
            .expectRevert("not the owner")
            .startMigration(ANYONE, 0, 1);
    }

    function test_operatorCanNotStartMigration() public {
        newTestCluster(vm)
            .expectRevert("not the owner")
            .startMigration(OPERATOR_A, 0, 1);
    }

    function test_ownerCanStartMigration() public {
        newTestCluster(vm)
            .startMigration(OWNER, 0, 1);
    }

    function test_canNotStartMigrationWithTooManyOperators() public {
        newTestCluster(vm)
            .generateMigration(0, MAX_OPERATORS - MIN_OPERATORS + 1)
            .expectRevert("too many operators")
            .startMigration(OWNER);
    }

    function test_canNotStartMigrationWithTooFewOperators() public {
        newTestCluster(vm)
            .generateMigration(1, 0)
            .expectRevert("too few operators")
            .startMigration(OWNER);
    }

    function test_canStartMigrationWhenAddedOperatorsReplenishRemoved() public {
        newTestCluster(vm)
            .generateMigration(MIN_OPERATORS, MIN_OPERATORS)
            .startMigration(OWNER);
    }

    function test_canNotStartMigrationWithTooManyNodes() public {
        newTestCluster(vm)
            .prepareMigration(0, 1)
            .addOperator(EXTRA_OPERATOR, MAX_NODES + 1)
            .expectRevert("too many nodes")
            .startMigration(OWNER);
    }

    function test_canNotStartMigrationWithTooFewNodes() public {
        newTestCluster(vm)
            .prepareMigration(0, 1)
            .addOperator(EXTRA_OPERATOR, MIN_NODES - 1)
            .expectRevert("too few nodes")
            .startMigration(OWNER);
    }

    function test_canNotStartMigrationWhenMaintenanceInProgress() public {
        newTestCluster(vm)
            .startMaintenance(OPERATOR_A)
            .expectRevert("maintenance in progress")
            .startMigration(OWNER, 0, 1);
    }

    function test_startMigrationBumpsVersion() public {
        newTestCluster(vm)
            .startMigration(OWNER, 0, 1)
            .assertVersion(1);
    }

    function test_startMigrationDoesNotBumpKeyspaceVersion() public {
        newTestCluster(vm)
            .startMigration(OWNER, 0, 1)
            .assertKeyspaceVersion(0);
    }

    // completeMigration

    function test_anyoneCanNotCompleteMigration() public {
        newTestCluster(vm)
            .startMigration(OWNER, 0, 1)
            .expectRevert("not pulling")
            .completeMigration(ANYONE);
    }

    function test_ownerCanNotCompleteMigration() public {
        newTestCluster(vm)
            .startMigration(OWNER, 0, 1)
            .expectRevert("not pulling")
            .completeMigration(OWNER);
    }

    function test_operatorCanNotCompleteNonExistentMigration() public {
        newTestCluster(vm)
            .expectRevert("not pulling")
            .completeMigration(OPERATOR_A);
    }

    function test_operatorCanCompleteMigration() public {
        newTestCluster(vm)
            .startMigration(OWNER, 0, 1)
            .completeMigration(OPERATOR_A);
    }

    function test_operatorCanNotCompleteMigrationTwice() public {
        newTestCluster(vm)
            .startMigration(OWNER, 0, 1)
            .completeMigration(OPERATOR_A)
            .expectRevert("not pulling")
            .completeMigration(OPERATOR_A);
    }

    function test_completeMigrationBumpsVersion() public {
        newTestCluster(vm)
            .startMigration(OWNER, 0, 1)
            .completeMigration(OPERATOR_A)
            .assertVersion(2);
    }

    function test_completeMigrationDoesNotBumpKeyspaceVersionIfNotCompleted() public {
        newTestCluster(vm)
            .startMigration(OWNER, 0, 1)
            .completeMigration(OPERATOR_A)
            .assertKeyspaceVersion(0);
    }

    function test_completeMigrationBumpKeyspaceVersionIfCompleted() public {
        newTestCluster(vm, Settings({ minOperators: 3, minNodes: uint8(MIN_NODES) }))
            .startMigration(OWNER, 0, 1)
            .completeMigration(OPERATOR_A)
            .completeMigration(OPERATOR_B)
            .completeMigration(OPERATOR_C)
            .completeMigration(EXTRA_OPERATOR)
            .assertKeyspaceVersion(1);
    }

    // startMaintenance 

    function test_anyoneCanNotStartMaintenance() public {
        newTestCluster(vm)
            .expectRevert("not an operator")
            .startMaintenance(ANYONE);
    }

    function test_ownerCanNotStartMaintenance() public {
        newTestCluster(vm)
            .expectRevert("not an operator")
            .startMaintenance(OWNER);
    }

    function test_operatorCanStartMaintenance() public {
        newTestCluster(vm)
            .startMaintenance(OPERATOR_A);
    }

    function test_operatorCanNotStartMaintenanceWhenMigrationInProgress() public {
        newTestCluster(vm)
            .startMigration(OWNER, 0, 1)
            .expectRevert("migration in progress")
            .startMaintenance(OPERATOR_A);
    }

    // copleteMaintenance

    function test_anyoneCanNotCompleteMaintenance() public {
        newTestCluster(vm)
            .expectRevert("not an operator")
            .startMaintenance(ANYONE);
    }

    function test_anotherOperatorCanNotCompleteMaintenance() public {
        newTestCluster(vm)
            .startMaintenance(OPERATOR_A)
            .expectRevert("not under maintenance")
            .completeMaintenance(OPERATOR_B);
    }

    function test_ownerCanNotCompleteMaintenance() public {
        newTestCluster(vm)
            .startMaintenance(OPERATOR_A)
            .expectRevert("not an operator")
            .completeMaintenance(OWNER);
    }

    function test_sameOperatorCanCompleteMaintenance() public {
        newTestCluster(vm)
            .startMaintenance(OPERATOR_A)
            .completeMaintenance(OPERATOR_A);
    }

    function test_canNotCompleteNonExistentMaintenance() public {
        newTestCluster(vm)
            .expectRevert("not under maintenance")
            .completeMaintenance(OPERATOR_A);
    }

    // abortMaintenance

    function test_anyoneCanNotAbortMaintenance() public {
        newTestCluster(vm)
            .startMaintenance(OPERATOR_A)
            .expectRevert("not the owner")
            .abortMaintenance(ANYONE);
    }

    function test_operatorCanNotAbortMaintenance() public {
        newTestCluster(vm)
            .startMaintenance(OPERATOR_A)
            .expectRevert("not the owner")
            .abortMaintenance(OPERATOR_B);
    }

    function test_ownerCanAbortMaintenance() public {
        newTestCluster(vm)
            .startMaintenance(OPERATOR_A)
            .abortMaintenance(OWNER);
    }
}

contract TestTools is Test {
    function assertEq(uint128 a, uint128 b) public pure {
        super.assertEq(uint256(a), uint256(b));
    }
}

struct TestCluster {
    Cluster inner;

    Vm vm;
    TestTools tools;

    address[] operatorsToRemove;
    uint256 operatorsToRemoveLen;

    NodeOperatorView[] operatorsToAdd;
    uint256 operatorsToAddLen;

    bytes revertBytes;
}

function newTestCluster(Vm vm) returns (TestCluster memory) {   
    return newTestCluster(vm, Settings({ minOperators: uint8(MIN_OPERATORS), minNodes: uint8(MIN_NODES) }));
}

function newTestCluster(Vm vm, Settings memory settings) returns (TestCluster memory) {   
    return newTestCluster(vm, settings, settings.minOperators, settings.minNodes);
}

function newTestCluster(Vm vm, uint256 operatorsCount, uint256 nodesCount) returns (TestCluster memory) {   
    return newTestCluster(vm, Settings({ minOperators: uint8(MIN_OPERATORS), minNodes: uint8(MIN_NODES) }), operatorsCount, nodesCount);
}

function newTestCluster(Vm vm, Settings memory settings, uint256 operatorsCount, uint256 nodesCount) returns (TestCluster memory) {
    NodeOperatorView[] memory operators = new NodeOperatorView[](operatorsCount);
    for (uint256 i = 0; i < operatorsCount; i++) {
        operators[i] = newNodeOperator(vm.addr(i + 1), nodesCount);
    }

    vm.prank(vm.addr(OWNER));

    return TestCluster({
        inner: new Cluster(settings, operators),
        vm: vm,
        tools: new TestTools(),
        operatorsToRemove: new address[](0),
        operatorsToRemoveLen: 0,
        operatorsToAdd: new NodeOperatorView[](0),
        operatorsToAddLen: 0,
        revertBytes: new bytes(0)
    });
}

library TestClusterLib {
    function setCaller(TestCluster memory self, uint256 callerPrivateKey) public returns (TestCluster memory) {
        self.vm.prank(self.vm.addr(callerPrivateKey));
        return self;
    }

    function expectRevert(TestCluster memory self, bytes memory revertBytes) public pure returns (TestCluster memory) {
        self.revertBytes = revertBytes;
        return self;
    }

    function assertVersion(TestCluster memory self, uint128 expectedVersion) public view returns (TestCluster memory){
        ClusterView memory clusterView = self.inner.getView();
        self.tools.assertEq(clusterView.version, expectedVersion);
        return self;
    }

    function assertKeyspaceVersion(TestCluster memory self, uint64 expectedVersion) public view returns (TestCluster memory){
        ClusterView memory clusterView = self.inner.getView();
        self.tools.assertEq(clusterView.keyspaceVersion, expectedVersion);
        return self;
    }

    function generateMigration(TestCluster memory self, uint256 operatorsToRemove, uint256 operatorsToAdd) public view returns (TestCluster memory) {
        prepareMigration(self, operatorsToRemove, operatorsToAdd);

        ClusterView memory clusterView = self.inner.getView();

        for (uint256 i = 0; i < clusterView.operators.slots.length; i++) {
            if (operatorsToRemove == 0) {
                break;
            }
            if (clusterView.operators.slots[i].addr != address(0)) {
                removeOperator(self, clusterView.operators.slots[i].addr);
                operatorsToRemove--;
            }
        }
        require(operatorsToRemove == 0);
        
        for (uint256 i = 0; i < operatorsToAdd; i++) {
            addOperator(self, EXTRA_OPERATOR + i);
        }

        return self;
    }

    function prepareMigration(TestCluster memory self, uint256 operatorsToRemove, uint256 operatorsToAdd) public pure returns (TestCluster memory) {
        self.operatorsToRemove = new address[](operatorsToRemove);
        self.operatorsToRemoveLen = 0;
        self.operatorsToAdd = new NodeOperatorView[](operatorsToAdd);
        self.operatorsToAddLen = 0;
        return self;
    }

    function addOperator(TestCluster memory self, uint256 privateKey) public pure returns (TestCluster memory) {
        return addOperator(self, privateKey, 2);
    }

    function addOperator(TestCluster memory self, uint256 privateKey, uint256 nodesCount) public pure returns (TestCluster memory) {
        Node[] memory nodes = new Node[](nodesCount);
        for (uint256 i = 0; i < nodesCount; i++) {
            nodes[i] = Node({ id: i + 1 , data: bytes("Some node specific data") });
        }

        self.operatorsToAdd[self.operatorsToAddLen] = NodeOperatorView({
            addr: self.vm.addr(privateKey),
            nodes: nodes,
            data: bytes("Some operator specific data")
        });
        self.operatorsToAddLen++;

        return self;
    }

    function removeOperator(TestCluster memory self, address operator) public pure returns (TestCluster memory) {
        self.operatorsToRemove[self.operatorsToRemoveLen] = operator;
        self.operatorsToRemoveLen++;
        return self;
    }

    function startMigration(TestCluster memory self, uint256 caller, uint256 operatorsToRemove, uint256 operatorsToAdd) public returns (TestCluster memory) {
        generateMigration(self, operatorsToRemove, operatorsToAdd);
        return startMigration(self, caller);
    }

    function startMigration(TestCluster memory self, uint256 caller) public returns (TestCluster memory) {
        setCaller(self, caller);
        if (self.revertBytes.length != 0) {
            self.vm.expectRevert(self.revertBytes);
        }
        self.inner.startMigration(self.operatorsToRemove, self.operatorsToAdd);
        return self;
    }

    function completeMigration(TestCluster memory self, uint256 caller) public returns (TestCluster memory) {
        setCaller(self, caller);
        if (self.revertBytes.length != 0) {
            self.vm.expectRevert(self.revertBytes);
        }
        self.inner.completeMigration();
        return self;
    }

    function startMaintenance(TestCluster memory self, uint256 caller) public returns (TestCluster memory) {
        setCaller(self, caller);
        if (self.revertBytes.length != 0) {
            self.vm.expectRevert(self.revertBytes);
        }
        self.inner.startMaintenance();
        return self;
    }

    function completeMaintenance(TestCluster memory self, uint256 caller) public returns (TestCluster memory) {
        setCaller(self, caller);
        if (self.revertBytes.length != 0) {
            self.vm.expectRevert(self.revertBytes);
        }
        self.inner.completeMaintenance();
        return self;
    }

    function abortMaintenance(TestCluster memory self, uint256 caller) public returns (TestCluster memory) {
        setCaller(self, caller);
        if (self.revertBytes.length != 0) {
            self.vm.expectRevert(self.revertBytes);
        }
        self.inner.abortMaintenance();
        return self;
    }
}

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

