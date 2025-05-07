// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Test} from "../dependencies/forge-std-1.9.7/src/Test.sol";
import {Vm} from "../dependencies/forge-std-1.9.7/src/Vm.sol";
import "../dependencies/forge-std-1.9.7/src/console.sol";

import "../src/Cluster/Cluster.sol";

contract ClusterTestSuite is Test {
    using TestClusterLib for TestCluster;

    mapping(address => uint256) privateKeys;

    constructor() {
        for (uint256 k = 1; k <= 10; k++) {
            privateKeys[vm.addr(k)] = k;
        }
    }

    function test_canNotCreateClusterWithTooFewOperators() public {
        vm.expectRevert("too few operators");
        newTestCluster(vm, Settings({ minOperators: 3, minNodes: 1 }), 2, 1);
    }

    function test_canNotCreateClusterWithTooFewNodes() public {
        vm.expectRevert("too few nodes");
        newTestCluster(vm, Settings({ minOperators: 3, minNodes: 2 }), 3, 1);
    }

    function test_canNotCreateClusterWithTooManyOperators() public {
        vm.expectRevert("too many operators");
        newTestCluster(vm, Settings({ minOperators: 3, minNodes: 1 }), 257, 1);
    }

    function test_canNotCreateClusterWithTooManyNodes() public {
        vm.expectRevert("too many nodes");
        newTestCluster(vm, Settings({ minOperators: 3, minNodes: 2 }), 3, 257);
    }

    function test_canCreateClusterWithMinNumberOfOperatorsAndNodes() public {
        newTestCluster(vm, Settings({ minOperators: 3, minNodes: 2 }), 3, 2);
    }

    function test_canCreateClusterWithMaxNumberOfOperators() public {
        newTestCluster(vm, Settings({ minOperators: 3, minNodes: 2 }), 256, 2);
    }

    function test_canCreateClusterWithMaxNumberOfNodes() public {
        newTestCluster(vm, Settings({ minOperators: 3, minNodes: 2 }), 3, 256);
    }

    function test_arbitraryKeypairCanNotStartMigration() public {
        newTestCluster(vm)
            .prepareMigration(0, 1)
            .addOperator(10)
            .setCaller(42)
            .startMigration("not the owner");
    }

    function test_memberKeypairCanNotStartMigration() public {
        newTestCluster(vm)
            .prepareMigration(0, 1)
            .addOperator(10)
            .setCaller(1)
            .startMigration("not the owner");
    }

    function test_ownerKeypairCanStartMigration() public {
        newTestCluster(vm)
            .prepareMigration(0, 1)
            .addOperator(10)
            .startMigration();
    }

    function test_canNotStartMigrationWithTooManyOperators() public {
        newTestCluster(vm, Settings({ minOperators: 3, minNodes: 2 }))
            .generateMigration(0, 256 - 3 + 1)
            .startMigration("too many operators");
    }

    function test_canNotStartMigrationWithTooFewOperators() public {
        newTestCluster(vm, Settings({ minOperators: 3, minNodes: 2 }))
            .generateMigration(2, 1)
            .startMigration("too few operators");
    }

    function test_canStartMigrationWhenAddedOperatorsReplenishRemoved() public {
        newTestCluster(vm, Settings({ minOperators: 3, minNodes: 2 }))
            .generateMigration(3, 3)
            .startMigration();
    }
}

struct TestCluster {
    Cluster inner;

    Vm vm; 

    address[] operatorsToRemove;
    uint256 operatorsToRemoveLen;

    NodeOperatorView[] operatorsToAdd;
    uint256 operatorsToAddLen;
}

function newTestCluster(Vm vm) returns (TestCluster memory) {   
    return newTestCluster(vm, Settings({ minOperators: 5, minNodes: 2 }));
}

function newTestCluster(Vm vm, Settings memory settings) returns (TestCluster memory) {   
    return newTestCluster(vm, settings, settings.minOperators, settings.minNodes);
}

function newTestCluster(Vm vm, Settings memory settings, uint256 operatorsCount, uint256 nodesCount) returns (TestCluster memory) {
    NodeOperatorView[] memory operators = new NodeOperatorView[](operatorsCount);
    for (uint256 i = 0; i < operatorsCount; i++) {
        operators[i] = newNodeOperator(vm.addr(i + 1), nodesCount);
    }

    return TestCluster({
        inner: new Cluster(settings, operators),
        vm: vm,
        operatorsToRemove: new address[](0),
        operatorsToRemoveLen: 0,
        operatorsToAdd: new NodeOperatorView[](0),
        operatorsToAddLen: 0
    });
}

library TestClusterLib {
    function setCaller(TestCluster memory self, uint256 callerPrivateKey) public returns (TestCluster memory) {
        self.vm.prank(self.vm.addr(callerPrivateKey));
        return self;
    }

    function generateMigration(TestCluster memory self, uint256 operatorsToRemove, uint operatorsToAdd) public view returns (TestCluster memory) {
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
            addOperator(self, i + 100);
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

    function startMigration(TestCluster memory self, bytes calldata revertBytes) public returns (TestCluster memory) {
        self.vm.expectRevert(revertBytes);
        startMigration(self);
        return self;
    }

    function startMigration(TestCluster memory self) public returns (TestCluster memory) {
        self.inner.startMigration(self.operatorsToRemove, self.operatorsToAdd);
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

