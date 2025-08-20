// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Test} from "forge-std/Test.sol";
import {UnsafeUpgrades} from "openzeppelin-foundry-upgrades/Upgrades.sol";
import {Cluster, NodeOperator, Settings, Bitmask} from "src/Cluster.sol";
import {keyspace} from "tests/Utils.sol";

contract MockClusterV2 is Cluster {
    uint256 public newVariable;
    string public constant CONTRACT_VERSION = "v2.0.0";
    
    function setNewVariable(uint256 _value) external {
        newVariable = _value;
    }
    
    function getContractVersion() external pure returns (string memory) {
        return CONTRACT_VERSION;
    }
    
    // Add a new function that doesn't exist in V1
    function isUpgraded() external pure returns (bool) {
        return true;
    }
}

contract ClusterIntegrationTest is Test {
    using Bitmask for uint256;

    Cluster cluster;
    address proxy;
    address owner;
    address newOwner;
    
    function _defaultSettings() internal pure returns (Settings memory) {
        return Settings({
            maxOperatorDataBytes: 1024,
            minOperators: 1,
            extra: ""
        });
    }
    
    function setUp() public {
        owner = address(0x1);
        newOwner = address(0x2);
        
        // Deploy upgradeable proxy
        NodeOperator[] memory initialOperators = new NodeOperator[](1);
        initialOperators[0] = NodeOperator({
            addr: address(0x100),
            data: "operator1"
        });
        
        vm.startPrank(owner);
        proxy = UnsafeUpgrades.deployUUPSProxy(
            address(new Cluster()),
            abi.encodeCall(Cluster.initialize, (_defaultSettings(), initialOperators))
        );
        cluster = Cluster(proxy);
        vm.stopPrank();
    }

    /*//////////////////////////////////////////////////////////////////////////
                            UUPS UPGRADE TESTS
    //////////////////////////////////////////////////////////////////////////*/

    function test_AuthorizeUpgrade_WhenCallerIsOwner() public {
        address newImpl = address(new MockClusterV2());
        
        vm.prank(owner);
        // Should not revert
        cluster.upgradeToAndCall(newImpl, "");
    }
    
    function test_AuthorizeUpgrade_WhenCallerIsNotOwner_ShouldRevert() public {
        address newImpl = address(new MockClusterV2());
        address nonOwner = address(0x999);
        
        vm.prank(nonOwner);
        vm.expectRevert(abi.encodeWithSignature("OwnableUnauthorizedAccount(address)", nonOwner));
        cluster.upgradeToAndCall(newImpl, "");
    }
    
    function test_UpgradePreservesState() public {
        // Add some state to the contract
        NodeOperator memory newOp = NodeOperator({
            addr: address(0x200),
            data: "operator2"
        });
        
        vm.prank(owner);
        cluster.addNodeOperator(newOp);
        
        uint16 countBefore = cluster.getOperatorCount();
        uint128 versionBefore = cluster.version();
        
        // Upgrade
        address newImpl = address(new MockClusterV2());
        vm.prank(owner);
        cluster.upgradeToAndCall(newImpl, "");
        
        MockClusterV2 upgradedCluster = MockClusterV2(proxy);
        
        // State should be preserved
        assertEq(upgradedCluster.getOperatorCount(), countBefore);
        assertEq(upgradedCluster.version(), versionBefore);
        
        // New functionality should work
        vm.prank(owner);
        upgradedCluster.setNewVariable(42);
        assertEq(upgradedCluster.newVariable(), 42);
        assertEq(upgradedCluster.getContractVersion(), "v2.0.0");
        assertTrue(upgradedCluster.isUpgraded());
    }

    /*//////////////////////////////////////////////////////////////////////////
                            OWNABLE2STEP TESTS  
    //////////////////////////////////////////////////////////////////////////*/

    function test_TransferOwnership_WhenCallerIsOwner() public {
        vm.prank(owner);
        cluster.transferOwnership(newOwner);
        
        // Ownership should not be transferred yet
        assertEq(cluster.owner(), owner);
        assertEq(cluster.pendingOwner(), newOwner);
    }
    
    function test_TransferOwnership_WhenCallerIsNotOwner_ShouldRevert() public {
        address nonOwner = address(0x999);
        
        vm.prank(nonOwner);
        vm.expectRevert(abi.encodeWithSignature("OwnableUnauthorizedAccount(address)", nonOwner));
        cluster.transferOwnership(newOwner);
    }
    
    function test_AcceptOwnership_WhenCallerIsPendingOwner() public {
        // Start ownership transfer
        vm.prank(owner);
        cluster.transferOwnership(newOwner);
        
        // Accept ownership
        vm.prank(newOwner);
        cluster.acceptOwnership();
        
        assertEq(cluster.owner(), newOwner);
        assertEq(cluster.pendingOwner(), address(0));
    }
    
    function test_AcceptOwnership_WhenCallerIsNotPendingOwner_ShouldRevert() public {
        address nonPendingOwner = address(0x999);
        
        vm.prank(owner);
        cluster.transferOwnership(newOwner);
        
        vm.prank(nonPendingOwner);
        vm.expectRevert(abi.encodeWithSignature("OwnableUnauthorizedAccount(address)", nonPendingOwner));
        cluster.acceptOwnership();
    }
    
    function test_OwnershipTransfer_MaintainsContractFunctionality() public {
        // Transfer ownership
        vm.prank(owner);
        cluster.transferOwnership(newOwner);
        
        vm.prank(newOwner);
        cluster.acceptOwnership();
        
        // New owner should be able to perform owner functions
        NodeOperator memory newOp = NodeOperator({
            addr: address(0x300),
            data: "operator3"
        });
        
        vm.prank(newOwner);
        cluster.addNodeOperator(newOp);
        
        assertEq(cluster.getOperatorCount(), 2);
    }
    
    function test_OldOwner_CannotPerformOwnerFunctions_AfterTransfer() public {
        // Transfer ownership
        vm.prank(owner);
        cluster.transferOwnership(newOwner);
        
        vm.prank(newOwner);
        cluster.acceptOwnership();
        
        // Old owner should not be able to perform owner functions
        NodeOperator memory newOp = NodeOperator({
            addr: address(0x400),
            data: "operator4"
        });
        
        vm.prank(owner);
        vm.expectRevert();
        cluster.addNodeOperator(newOp);
    }

    /*//////////////////////////////////////////////////////////////////////////
                            CROSS-FUNCTIONALITY TESTS
    //////////////////////////////////////////////////////////////////////////*/
    
    function test_OwnershipTransfer_DuringMigration() public {
        // Start a migration
        uint8[] memory newSlots = new uint8[](1);
        newSlots[0] = 0;
        
        vm.prank(owner);
        cluster.startMigration(keyspace(newSlots, 1));
        
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
    
    function test_Upgrade_DuringMigration() public {
        // Start a migration
        uint8[] memory newSlots = new uint8[](1);
        newSlots[0] = 0;
        
        vm.prank(owner);
        cluster.startMigration(keyspace(newSlots, 1));
        
        // Upgrade should work during migration
        address newImpl = address(new MockClusterV2());
        vm.prank(owner);
        cluster.upgradeToAndCall(newImpl, "");
        
        MockClusterV2 upgradedCluster = MockClusterV2(proxy);
        
        // Migration state should be preserved
        (,, bool inProgress) = upgradedCluster.getMigrationStatus();
        assertTrue(inProgress);
    }
} 
