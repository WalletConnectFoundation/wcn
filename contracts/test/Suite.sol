pragma solidity ^0.8.20;

import {Test} from "../dependencies/forge-std-1.9.7/src/Test.sol";
import "../src/Cluster/Cluster.sol";

contract ClusterTestSuite is Test {
    mapping(address => uint256) privateKeys;

    function newNodeOperator(uint256 privateKey, uint256 nodesCount) internal pure returns (NodeOperatorView memory) {
        Node[] memory nodes = new Node[](nodesCount);
        for (uint256 i = 0; i < nodesCount; i++) {
            nodes[i] = Node({ id: i + 1 , data: bytes("Some node specific data") });
        }

        return NodeOperatorView({
            addr: vm.addr(privateKey),
            nodes: nodes,
            data: bytes("Some operator specific data")
        });
    }

    function newCluster() internal returns (Cluster) {   
        return newCluster(Settings({ minOperators: 5, minNodes: 2 }));
    }

    function newCluster(Settings memory settings) internal returns (Cluster) {   
        return newCluster(settings, settings.minOperators, settings.minNodes);
    }

    function newCluster(Settings memory settings, uint256 operatorsCount, uint256 nodesCount) internal returns (Cluster) {
        NodeOperatorView[] memory operators = new NodeOperatorView[](operatorsCount);
        for (uint256 i = 0; i < operatorsCount; i++) {
            operators[i] = newNodeOperator(i + 1, nodesCount);
        }
    
        return new Cluster(settings, operators);
    }

    constructor() {
        for (uint256 k = 1; k <= 10; k++) {
            privateKeys[vm.addr(k)] = k;
        }
    }

    function test_canNotCreateClusterWithTooFewOperators() public {
        vm.expectRevert("too few operators");
        newCluster(Settings({ minOperators: 3, minNodes: 1 }), 2, 1);
    }

    function test_canNotCreateClusterWithTooFewNodes() public {
        vm.expectRevert("too few nodes");
        newCluster(Settings({ minOperators: 3, minNodes: 2 }), 3, 1);
    }

    function test_canNotCreateClusterWithTooManyOperators() public {
        vm.expectRevert("too many operators");
        newCluster(Settings({ minOperators: 3, minNodes: 1 }), 257, 1);
    }

    function test_canNotCreateClusterWithTooManyNodes() public {
        vm.expectRevert("too many nodes");
        newCluster(Settings({ minOperators: 3, minNodes: 2 }), 3, 257);
    }

    function test_canCreateClusterWithMinNumberOfOperatorsAndNodes() public {
        newCluster(Settings({ minOperators: 3, minNodes: 2 }), 3, 2);
    }

    function test_canCreateClusterWithMaxNumberOfOperators() public {
        newCluster(Settings({ minOperators: 3, minNodes: 2 }), 256, 2);
    }

    function test_canCreateClusterWithMaxNumberOfNodes() public {
        newCluster(Settings({ minOperators: 3, minNodes: 2 }), 3, 256);
    }

    // function startMigration(Cluster cluster, uint256 callerPrivateKey, uint256 removeCount, uint256 addCount) {
    //     vm.prank(vm.addr(callerPrivateKey));

    //     address[] operatorsToRemove = new address[](removeCount);
    //     for (uint256 i = 0; i < removeCount; i++) {
    //         operatorsToRemove[i] = ;
    //     }

    //     NodeOperatorView[] operatorsToAdd = new NodeOperatorView[](addCount);
        
    //     cluster.startMigration()
    // }

    // function test_arbitraryKeypairCanNotStartMigration() public {
    //     Cluster cluster = newCluster();
    //     newCluster(Settings({ minOperators: 3, minNodes: 2 }), 3, 256);
    // }

    // uint256 testNumber;

    // function setUp() public {
    //     testNumber = 42;
    // }

    // function test_NumberIs42() public {
    //     assertEq(testNumber, 42);
    // }

    // /// forge-config: default.allow_internal_expect_revert = true
    // function testRevert_Subtract43() public {
    //     vm.expectRevert();
    //     testNumber -= 43;
    // }
}
