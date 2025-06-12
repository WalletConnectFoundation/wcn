// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Test} from "forge-std/Test.sol";
import {OwnableUpgradeable} from "@openzeppelin/contracts-upgradeable/access/OwnableUpgradeable.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {ClusterHarness} from "tests/unit/ClusterHarness.sol";
import {Cluster, Settings, NodeOperator, NodeOperatorData, ClusterView} from "src/Cluster.sol";

contract ClusterTest is Test {
    ClusterHarness public cluster;
    
    // Events from Cluster contract
    event ClusterInitialized(NodeOperator[] operators, Settings settings, uint128 version);
    event NodeOperatorAdded(address indexed operator, uint8 indexed slot, NodeOperatorData operatorData, uint128 version);
    event NodeOperatorUpdated(address indexed operator, uint8 indexed slot, NodeOperatorData operatorData, uint128 version);
    event NodeOperatorRemoved(address indexed operator, uint8 indexed slot, uint128 version);
    event MaintenanceToggled(address indexed operator, bool active, uint128 version);
    event MigrationStarted(uint64 indexed id, uint8[] operators, uint8 replicationStrategy, uint64 keyspaceVersion, uint128 version);
    event MigrationDataPullCompleted(uint64 indexed id, address indexed operator, uint128 version);
    event MigrationCompleted(uint64 indexed id, address indexed operator, uint128 version);
    event MigrationAborted(uint64 indexed id, uint128 version);
    event SettingsUpdated(Settings newSettings, address indexed updatedBy, uint128 version);
    
    function _defaultSettings() internal pure returns (Settings memory) {
        return Settings({
            maxOperatorDataBytes: 4096,
            minOperators: 1
        });
    }
    
    function _operator(uint256 index) internal pure returns (NodeOperator memory) {
        return NodeOperator({
            addr: address(uint160(0x1000 + index)),
            data: abi.encodePacked("operator", index),
            maintenance: false
        });
    }
    
    address constant OWNER = address(0x1);
    address constant OPERATOR1 = address(0x2);
    address constant OPERATOR2 = address(0x3);
    address constant NON_OWNER = address(0x4);
    
    function setUp() public {
        cluster = _deployFreshCluster(2);
    }
    
    function _deployFreshCluster(uint256 operatorCount) internal returns (ClusterHarness) {
        NodeOperator[] memory operators = new NodeOperator[](operatorCount);
        for (uint256 i = 0; i < operatorCount; i++) {
            operators[i] = _operator(i + 1);
        }

        ClusterHarness implementation = new ClusterHarness();
        bytes memory initData = abi.encodeCall(Cluster.initialize, (_defaultSettings(), operators));
        
        vm.prank(OWNER);
        ERC1967Proxy proxy = new ERC1967Proxy(address(implementation), initData);
        
        return ClusterHarness(address(proxy));
    }
    
    function _createInitialOperators(uint256 count) internal pure returns (NodeOperator[] memory) {
        NodeOperator[] memory operators = new NodeOperator[](count);
        for (uint256 i = 0; i < count; i++) {
            operators[i] = NodeOperator({
                addr: address(uint160(0x1000 + i)),
                data: abi.encodePacked("operator", i),
                maintenance: false
            });
        }
        return operators;
    }
    
    function _getSortedOperatorSlots(uint256 count) internal pure returns (uint8[] memory) {
        uint8[] memory slots = new uint8[](count);
        for (uint256 i = 0; i < count; i++) {
            slots[i] = uint8(i);
        }
        return slots;
    }
    function test_InitializeWhenInitialOperatorsLengthExceedsMAX_OPERATORS() external {
        // It should revert with TooManyOperators.
        NodeOperator[] memory tooManyOperators = new NodeOperator[](256);
        for (uint256 i = 0; i < 256; i++) {
            tooManyOperators[i] = NodeOperator({
                addr: address(uint160(0x2000 + i)),
                data: abi.encodePacked("op", i),
                maintenance: false
            });
        }
        
        ClusterHarness implementation = new ClusterHarness();
        bytes memory initData = abi.encodeCall(Cluster.initialize, (_defaultSettings(), tooManyOperators));
        
        vm.expectRevert(Cluster.TooManyOperators.selector);
        vm.prank(OWNER);
        new ERC1967Proxy(address(implementation), initData);
    }

    function test_InitializeWhenInitialOperatorsLengthIsLessThanMinOperators() external {
        // It should revert with InsufficientOperators.
        Settings memory strictSettings = Settings({
            maxOperatorDataBytes: 4096,
            minOperators: 3
        });
        NodeOperator[] memory tooFewOperators = _createInitialOperators(2);
        
        ClusterHarness implementation = new ClusterHarness();
        bytes memory initData = abi.encodeCall(Cluster.initialize, (strictSettings, tooFewOperators));
        
        vm.expectRevert(Cluster.InsufficientOperators.selector);
        vm.prank(OWNER);
        new ERC1967Proxy(address(implementation), initData);
    }

    function test_InitializeWhenOperatorDataIsEmpty() external {
        // It should revert with InvalidOperatorData.
        NodeOperator[] memory operators = new NodeOperator[](1);
        operators[0] = NodeOperator({
            addr: OPERATOR1,
            data: "",  // Empty data
            maintenance: false
        });
        
        ClusterHarness implementation = new ClusterHarness();
        bytes memory initData = abi.encodeCall(Cluster.initialize, (_defaultSettings(), operators));
        
        vm.expectRevert(Cluster.InvalidOperatorData.selector);
        vm.prank(OWNER);
        new ERC1967Proxy(address(implementation), initData);
    }

    function test_InitializeWhenOperatorDataExceedsMaxOperatorDataBytes() external {
        // It should revert with InvalidOperatorData.
        Settings memory smallSettings = Settings({
            maxOperatorDataBytes: 10,
            minOperators: 1
        });
        NodeOperator[] memory operators = new NodeOperator[](1);
        operators[0] = NodeOperator({
            addr: OPERATOR1,
            data: "this data is way too long for the limit",  // Exceeds 10 bytes
            maintenance: false
        });
        
        ClusterHarness implementation = new ClusterHarness();
        bytes memory initData = abi.encodeCall(Cluster.initialize, (smallSettings, operators));
        
        vm.expectRevert(Cluster.InvalidOperatorData.selector);
        vm.prank(OWNER);
        new ERC1967Proxy(address(implementation), initData);
    }

    function test_InitializeWhenInputsAreValid() external {
        // It should set operatorCount correctly.
        // It should initialize first keyspace with all operators.
        // It should emit Cluster.
        // It should set version to 0.
        NodeOperator[] memory operators = _createInitialOperators(3);
        
        ClusterHarness implementation = new ClusterHarness();
        bytes memory initData = abi.encodeCall(Cluster.initialize, (_defaultSettings(), operators));
        
        vm.expectEmit(true, true, true, true);
        emit ClusterInitialized(operators, _defaultSettings(), 1);
        
        vm.prank(OWNER);
        ERC1967Proxy proxy = new ERC1967Proxy(address(implementation), initData);
        ClusterHarness freshCluster = ClusterHarness(address(proxy));
        
        assertEq(freshCluster.getOperatorCount(), 3);
        assertEq(freshCluster.version(), 1);
        (uint8[] memory keyspaceMembers,) = freshCluster.getCurrentKeyspace();
        assertEq(keyspaceMembers.length, 3);
        assertTrue(freshCluster.isInitialized());
    }

    function test_AddNodeOperatorWhenCallerIsNotOwner() external {
        // It should revert with OwnableUnauthorizedAccount.
        NodeOperator memory newOp = NodeOperator({
            addr: address(0x999),
            data: "new operator",
            maintenance: false
        });
        
        vm.expectRevert(abi.encodeWithSelector(OwnableUpgradeable.OwnableUnauthorizedAccount.selector, NON_OWNER));
        vm.prank(NON_OWNER);
        cluster.addNodeOperator(newOp);
    }

    function test_AddNodeOperatorWhenOperatorCountAlreadyAtMAX_OPERATORS() external {
        // It should revert with TooManyOperators.
        // Create a cluster at max capacity first
        NodeOperator[] memory maxOperators = new NodeOperator[](255);
        for (uint256 i = 0; i < 255; i++) {
            maxOperators[i] = NodeOperator({
                addr: address(uint160(0x3000 + i)),
                data: abi.encodePacked("max_op", i),
                maintenance: false
            });
        }
        
        ClusterHarness implementation = new ClusterHarness();
        bytes memory initData = abi.encodeCall(Cluster.initialize, (_defaultSettings(), maxOperators));
        
        vm.prank(OWNER);
        ERC1967Proxy proxy = new ERC1967Proxy(address(implementation), initData);
        ClusterHarness maxCluster = ClusterHarness(address(proxy));
        
        NodeOperator memory extraOp = NodeOperator({
            addr: address(0x9999),
            data: "extra operator",
            maintenance: false
        });
        
        vm.expectRevert(Cluster.TooManyOperators.selector);
        vm.prank(OWNER);
        maxCluster.addNodeOperator(extraOp);
    }

    function test_AddNodeOperatorWhenOperatorAddressIsZero() external {
        // It should revert with InvalidOperator.
        NodeOperator memory zeroAddressOp = NodeOperator({
            addr: address(0), // Zero address
            data: "zero address operator",
            maintenance: false
        });
        
        vm.expectRevert(Cluster.InvalidOperator.selector);
        vm.prank(OWNER);
        cluster.addNodeOperator(zeroAddressOp);
    }

    function test_AddNodeOperatorWhenOperatorAlreadyExists() external {
        // It should revert with OperatorExists.
        NodeOperator memory existingOp = NodeOperator({
            addr: address(uint160(0x1001)), // Same as first operator in setup (i+1 from _operator)
            data: "duplicate operator",
            maintenance: false
        });
        
        vm.expectRevert(Cluster.OperatorExists.selector);
        vm.prank(OWNER);
        cluster.addNodeOperator(existingOp);
    }

    function test_AddNodeOperatorWhenOperatorDataIsInvalid() external {
        // It should revert with InvalidOperatorData.
        NodeOperator memory invalidOp = NodeOperator({
            addr: address(0x999),
            data: "", // Empty data is invalid
            maintenance: false
        });
        
        vm.expectRevert(Cluster.InvalidOperatorData.selector);
        vm.prank(OWNER);
        cluster.addNodeOperator(invalidOp);
    }

    function test_AddNodeOperatorWhenInputsAreValid() external {
        // It should increase operatorCount.
        // It should assign next available slot.
        // It should emit NodeOperatorAdded.
        // It should increment version.
        // It should not affect keyspaceVersion.
        uint8 initialCount = cluster.getOperatorCount();
        uint128 initialVersion = cluster.version();
        uint64 initialKeyspaceVersion = cluster.keyspaceVersion();
        
        NodeOperator memory newOp = NodeOperator({
            addr: address(0x999),
            data: "new valid operator",
            maintenance: false
        });
        
        vm.expectEmit(true, true, true, true);
        emit NodeOperatorAdded(newOp.addr, 2, NodeOperatorData({data: newOp.data, maintenance: newOp.maintenance}), initialVersion + 1);
        
        vm.prank(OWNER);
        cluster.addNodeOperator(newOp);
        
        assertEq(cluster.getOperatorCount(), initialCount + 1);
        assertTrue(cluster.isOperator(newOp.addr));
        assertEq(cluster.version(), initialVersion + 1);
        assertEq(cluster.keyspaceVersion(), initialKeyspaceVersion);
    }

    function test_UpdateNodeOperatorWhenClusterIsNotInitialized() external {
        // It should revert with NotInitialized.
        ClusterHarness uninitializedCluster = new ClusterHarness();
        NodeOperator memory operator = _operator(1);
        
        vm.expectRevert(Cluster.NotInitialized.selector);
        vm.prank(OWNER);
        uninitializedCluster.updateNodeOperator(operator);
    }

    function test_UpdateNodeOperatorWhenCallerIsNeitherOperatorNorOwner() external {
        // It should revert with Unauthorized.
        NodeOperator memory operator = NodeOperator({
            addr: address(uint160(0x1001)), // Existing operator
            data: "updated data",
            maintenance: false
        });
        
        vm.expectRevert(Cluster.Unauthorized.selector);
        vm.prank(NON_OWNER); // Neither operator nor owner
        cluster.updateNodeOperator(operator);
    }

    function test_UpdateNodeOperatorWhenOperatorDoesNotExist() external {
        // It should revert with OperatorNotFound.
        NodeOperator memory nonExistentOperator = NodeOperator({
            addr: address(0x9999), // Non-existent operator
            data: "some data",
            maintenance: false
        });
        
        vm.expectRevert(Cluster.OperatorNotFound.selector);
        vm.prank(OWNER);
        cluster.updateNodeOperator(nonExistentOperator);
    }

    function test_UpdateNodeOperatorWhenOperatorDataIsInvalid() external {
        // It should revert with InvalidOperatorData.
        NodeOperator memory invalidOperator = NodeOperator({
            addr: address(uint160(0x1001)), // Existing operator
            data: "", // Invalid empty data
            maintenance: false
        });
        
        vm.expectRevert(Cluster.InvalidOperatorData.selector);
        vm.prank(OWNER);
        cluster.updateNodeOperator(invalidOperator);
    }

    function test_UpdateNodeOperatorWhenInputsAreValid() external {
        // It should update operator data.
        // It should emit NodeOperatorUpdated.
        // It should increment version.
        // It should not affect operatorCount.
        NodeOperator memory updatedOperator = NodeOperator({
            addr: address(uint160(0x1001)), // Existing operator
            data: "updated operator data",
            maintenance: true
        });
        
        uint128 versionBefore = cluster.version();
        uint8 operatorCountBefore = cluster.getOperatorCount();
        
        vm.expectEmit(true, true, true, true);
        emit NodeOperatorUpdated(updatedOperator.addr, 0, NodeOperatorData({data: updatedOperator.data, maintenance: updatedOperator.maintenance}), versionBefore + 1);
        
        vm.prank(OWNER);
        cluster.updateNodeOperator(updatedOperator);
        
        assertEq(cluster.version(), versionBefore + 1);
        assertEq(cluster.getOperatorCount(), operatorCountBefore); // Should not change
        
        // Verify data was updated
        ClusterView memory clusterView = cluster.getView();
        bool found = false;
        for (uint256 i = 0; i < clusterView.operators.length; i++) {
            if (clusterView.operators[i] == updatedOperator.addr) {
                assertEq(clusterView.operatorData[i].data, updatedOperator.data);
                assertEq(clusterView.operatorData[i].maintenance, updatedOperator.maintenance);
                found = true;
                break;
            }
        }
        assertTrue(found);
    }

    function test_RemoveNodeOperatorWhenCallerIsNotOwner() external {
        // It should revert with OwnableUnauthorizedAccount.
        address operatorToRemove = address(uint160(0x1001));
        
        vm.expectRevert(abi.encodeWithSelector(OwnableUpgradeable.OwnableUnauthorizedAccount.selector, NON_OWNER));
        vm.prank(NON_OWNER);
        cluster.removeNodeOperator(operatorToRemove);
    }

    function test_RemoveNodeOperatorWhenOperatorDoesNotExist() external {
        // It should revert with OperatorNotFound.
        address nonExistentOperator = address(0x9999);
        
        vm.expectRevert(Cluster.OperatorNotFound.selector);
        vm.prank(OWNER);
        cluster.removeNodeOperator(nonExistentOperator);
    }

    function test_RemoveNodeOperatorWhenRemovalWouldViolateMinOperators() external {
        // It should revert with InsufficientOperators.
        // Update settings to require more operators than we have after removal
        Settings memory strictSettings = Settings({
            maxOperatorDataBytes: 4096,
            minOperators: 2 // Same as current count, removal would violate this
        });
        
        vm.prank(OWNER);
        cluster.updateSettings(strictSettings);
        
        address operatorToRemove = address(uint160(0x1001));
        
        vm.expectRevert(Cluster.InsufficientOperators.selector);
        vm.prank(OWNER);
        cluster.removeNodeOperator(operatorToRemove);
    }

    function test_RemoveNodeOperatorWhenOperatorIsInCurrentKeyspace() external {
        // It should revert with OperatorInKeyspace.
        // The operator is already in the current keyspace by default
        address operatorToRemove = address(uint160(0x1001)); // This operator is in slot 0, which is in current keyspace
        
        vm.expectRevert(Cluster.OperatorInKeyspace.selector);
        vm.prank(OWNER);
        cluster.removeNodeOperator(operatorToRemove);
    }

    function test_RemoveNodeOperatorWhenInputsAreValid() external {
        // It should decrease operatorCount.
        // It should free the operator slot.
        // It should emit NodeOperatorRemoved.
        // It should increment version.
        
        // First add a third operator that's not in the keyspace
        NodeOperator memory newOp = NodeOperator({
            addr: address(0x999),
            data: "removable operator",
            maintenance: false
        });
        
        vm.prank(OWNER);
        cluster.addNodeOperator(newOp);
        
        uint8 operatorCountBefore = cluster.getOperatorCount();
        uint128 versionBefore = cluster.version();
        
        vm.expectEmit(true, true, true, true);
        emit NodeOperatorRemoved(newOp.addr, 2, versionBefore + 1); // Should be in slot 2
        
        vm.prank(OWNER);
        cluster.removeNodeOperator(newOp.addr);
        
        assertEq(cluster.getOperatorCount(), operatorCountBefore - 1);
        assertEq(cluster.version(), versionBefore + 1);
        assertFalse(cluster.isOperator(newOp.addr));
    }

    function test_SetMaintenanceWhenClusterIsNotInitialized() external {
        // It should revert with NotInitialized.
        ClusterHarness uninitializedCluster = new ClusterHarness();
        
        vm.expectRevert(Cluster.NotInitialized.selector);
        vm.prank(OWNER);
        uninitializedCluster.setMaintenance(true);
    }

    function test_SetMaintenanceWhenStartingMaintenanceAndAnotherMaintenanceIsActive() external {
        // It should revert with MaintenanceInProgress.
        vm.prank(OWNER);
        cluster.setMaintenance(true);
        
        vm.expectRevert(Cluster.MaintenanceInProgress.selector);
        vm.prank(address(uint160(0x1000))); // First operator
        cluster.setMaintenance(true);
    }

    function test_SetMaintenanceWhenEndingMaintenanceAndCallerIsNotTheOneWhoStarted() external {
        // It should revert with Unauthorized.
        vm.prank(OWNER);
        cluster.setMaintenance(true);
        
        vm.expectRevert(Cluster.Unauthorized.selector);
        vm.prank(address(uint160(0x1000))); // First operator tries to end owner's maintenance
        cluster.setMaintenance(false);
    }

    function test_SetMaintenanceWhenCallerIsNotAnOperatorAndNotOwner() external {
        // It should revert with Unauthorized.
        vm.expectRevert(Cluster.Unauthorized.selector);
        vm.prank(NON_OWNER);
        cluster.setMaintenance(true);
    }

    function test_SetMaintenanceWhenInputsAreValid() external {
        // It should update maintenance state.
        // It should emit MaintenanceToggled.
        // It should increment version.
        // It should not affect keyspaceVersion.
        uint128 initialVersion = cluster.version();
        uint64 initialKeyspaceVersion = cluster.keyspaceVersion();
        
        vm.expectEmit(true, true, true, true);
        emit MaintenanceToggled(OWNER, true, initialVersion + 1);
        
        vm.prank(OWNER);
        cluster.setMaintenance(true);
        
        assertEq(cluster.version(), initialVersion + 1);
        assertEq(cluster.keyspaceVersion(), initialKeyspaceVersion);
        
        // Check maintenance state
        ClusterView memory clusterView = cluster.getView();
        assertEq(clusterView.maintenance.slot, OWNER);
        assertTrue(clusterView.maintenance.isOwnerMaintenance);
    }

    function test_StartMigrationWhenCallerIsNotOwner() external {
        // It should revert with OwnableUnauthorizedAccount.
        uint8[] memory operatorSlots = _getSortedOperatorSlots(1);
        
        vm.expectRevert(abi.encodeWithSelector(OwnableUpgradeable.OwnableUnauthorizedAccount.selector, NON_OWNER));
        vm.prank(NON_OWNER);
        cluster.startMigration(operatorSlots, 0);
    }

    function test_StartMigrationWhenClusterIsNotInitialized() external {
        // It should revert with NotInitialized.
        // Create a cluster with minOperators = 0, then remove all operators to make it uninitialized
        ClusterHarness tempCluster = _deployFreshCluster(1);
        
        // Update settings to allow 0 operators 
        Settings memory allowEmptySettings = Settings({
            maxOperatorDataBytes: 4096,
            minOperators: 0
        });
        
        vm.prank(OWNER);
        tempCluster.updateSettings(allowEmptySettings);
        
        // Start empty migration to remove operator from keyspace
        uint8[] memory emptySlots = new uint8[](0);
        vm.prank(OWNER);
        tempCluster.startMigration(emptySlots, 0);
        
        // Remove the only operator to make cluster uninitialized
        vm.prank(OWNER);
        tempCluster.removeNodeOperator(address(uint160(0x1001)));
        
        uint8[] memory operatorSlots = new uint8[](1);
        operatorSlots[0] = 0;
        
        vm.expectRevert(Cluster.NotInitialized.selector);
        vm.prank(OWNER);
        tempCluster.startMigration(operatorSlots, 0);
    }

    function test_StartMigrationWhenMaintenanceIsInProgress() external {
        // It should revert with MaintenanceInProgress.
        vm.prank(OWNER);
        cluster.setMaintenance(true);
        
        uint8[] memory operatorSlots = _getSortedOperatorSlots(1);
        
        vm.expectRevert(Cluster.MaintenanceInProgress.selector);
        vm.prank(OWNER);
        cluster.startMigration(operatorSlots, 0);
    }

    function test_StartMigrationWhenMigrationIsAlreadyInProgress() external {
        // It should revert with MigrationInProgress.
        uint8[] memory operatorSlots = _getSortedOperatorSlots(1);
        
        vm.prank(OWNER);
        cluster.startMigration(operatorSlots, 0); // Start first migration
        
        vm.expectRevert(Cluster.MigrationInProgress.selector);
        vm.prank(OWNER);
        cluster.startMigration(operatorSlots, 0); // Try to start another
    }

    function test_StartMigrationWhenNewOperatorSlotsExceedsMAX_OPERATORS() external {
        // It should revert with TooManyOperators.
        uint8[] memory tooManySlots = new uint8[](256); // Exceeds MAX_OPERATORS (255)
        for (uint256 i = 0; i < 256; i++) {
            tooManySlots[i] = uint8(i);
        }
        
        vm.expectRevert(Cluster.TooManyOperators.selector);
        vm.prank(OWNER);
        cluster.startMigration(tooManySlots, 0);
    }

    function test_StartMigrationWhenNewOperatorSlotsAreNotSorted() external {
        // It should revert with InvalidOperator.
        uint8[] memory unsortedSlots = new uint8[](2);
        unsortedSlots[0] = 1;
        unsortedSlots[1] = 0; // Not sorted
        
        vm.expectRevert(Cluster.InvalidOperator.selector);
        vm.prank(OWNER);
        cluster.startMigration(unsortedSlots, 0);
    }

    function test_StartMigrationWhenSlotDoesNotExist() external {
        // It should revert with OperatorNotFound.
        uint8[] memory nonExistentSlots = new uint8[](1);
        nonExistentSlots[0] = 99; // Slot 99 doesn't exist
        
        vm.expectRevert(Cluster.OperatorNotFound.selector);
        vm.prank(OWNER);
        cluster.startMigration(nonExistentSlots, 0);
    }

    function test_StartMigrationWhenKeyspaceIsIdenticalToCurrent() external {
        // It should revert with SameKeyspace.
        uint8[] memory currentSlots = _getSortedOperatorSlots(2); // Same as current keyspace
        
        vm.expectRevert(Cluster.SameKeyspace.selector);
        vm.prank(OWNER);
        cluster.startMigration(currentSlots, 0); // Same slots and strategy
    }

    function test_StartMigrationWhenInputsAreValid() external {
        // It should increment migrationId.
        // It should increment keyspaceVersion.
        // It should set pulling flags.
        // It should emit MigrationStarted.
        // It should increment version.
        uint128 initialVersion = cluster.version();
        uint64 initialKeyspaceVersion = cluster.keyspaceVersion();
        uint8[] memory operatorSlots = _getSortedOperatorSlots(1); // Migrate to just one operator
        
        vm.expectEmit(true, true, true, true);
        emit MigrationStarted(1, operatorSlots, 0, initialKeyspaceVersion + 1, initialVersion + 1);
        
        vm.prank(OWNER);
        cluster.startMigration(operatorSlots, 0);
        
        assertEq(cluster.version(), initialVersion + 1);
        assertEq(cluster.keyspaceVersion(), initialKeyspaceVersion + 1);
        
        (uint64 migrationId, uint16 remaining, bool inProgress) = cluster.getMigrationStatus();
        assertEq(migrationId, 1);
        assertEq(remaining, 1);
        assertTrue(inProgress);
        
        uint8[] memory pullingOps = cluster.getPullingOperators();
        assertEq(pullingOps.length, 1);
        assertEq(pullingOps[0], 0);
    }

    function test_CompleteMigrationWhenClusterIsNotInitialized() external {
        // It should revert with NotInitialized.
        ClusterHarness uninitializedCluster = new ClusterHarness();
        
        vm.expectRevert(Cluster.NotInitialized.selector);
        vm.prank(OWNER);
        uninitializedCluster.completeMigration(1);
    }

    function test_CompleteMigrationWhenNoMigrationIsInProgress() external {
        // It should revert with NoMigrationInProgress.
        vm.expectRevert(Cluster.NoMigrationInProgress.selector);
        vm.prank(address(uint160(0x1001))); // First operator address
        cluster.completeMigration(1);
    }

    function test_CompleteMigrationWhenMigrationIdIsWrong() external {
        // It should revert with WrongMigrationId.
        uint8[] memory operatorSlots = _getSortedOperatorSlots(1);
        
        vm.prank(OWNER);
        cluster.startMigration(operatorSlots, 0);
        
        vm.expectRevert(Cluster.WrongMigrationId.selector);
        vm.prank(address(uint160(0x1001))); // First operator address
        cluster.completeMigration(999); // Wrong migration ID
    }

    function test_CompleteMigrationWhenCallerIsNotAnOperator() external {
        // It should revert with CallerNotOperator.
        uint8[] memory operatorSlots = _getSortedOperatorSlots(1);
        
        vm.prank(OWNER);
        cluster.startMigration(operatorSlots, 0);
        
        vm.expectRevert(Cluster.CallerNotOperator.selector);
        vm.prank(NON_OWNER); // Not an operator
        cluster.completeMigration(1);
    }

    function test_CompleteMigrationWhenOperatorIsNotPulling() external {
        // It should revert with OperatorNotPulling.
        uint8[] memory operatorSlots = _getSortedOperatorSlots(1); // Only first operator is pulling
        
        vm.prank(OWNER);
        cluster.startMigration(operatorSlots, 0);
        
        vm.expectRevert(Cluster.OperatorNotPulling.selector);
        vm.prank(address(uint160(0x1002))); // Second operator (not pulling)
        cluster.completeMigration(1);
    }

    modifier whenInputsAreValid() {
        _;
    }

    function test_CompleteMigrationWhenInputsAreValid() external whenInputsAreValid {
        // It should clear operator pulling state.
        // It should decrement pullingCount.
        uint8[] memory operatorSlots = _getSortedOperatorSlots(1); // Different from current keyspace
        
        vm.prank(OWNER);
        cluster.startMigration(operatorSlots, 1); // Different replication strategy
        
        (,uint16 remainingBefore,) = cluster.getMigrationStatus();
        
        vm.prank(address(uint160(0x1001))); // First operator
        cluster.completeMigration(1);
        
        (,uint16 remainingAfter,) = cluster.getMigrationStatus();
        assertEq(remainingAfter, remainingBefore - 1);
    }

    function test_CompleteMigrationWhenLastOperatorCompletes() external whenInputsAreValid {
        // It should emit MigrationCompleted.
        // It should increment version.
        uint8[] memory operatorSlots = _getSortedOperatorSlots(1); // Only one operator
        
        vm.prank(OWNER);
        cluster.startMigration(operatorSlots, 0);
        
        uint128 versionBefore = cluster.version();
        
        vm.expectEmit(true, true, true, true);
        emit MigrationCompleted(1, address(uint160(0x1001)), versionBefore + 1);
        
        vm.prank(address(uint160(0x1001))); // First operator completes (last one)
        cluster.completeMigration(1);
        
        assertEq(cluster.version(), versionBefore + 1);
        (,,bool inProgress) = cluster.getMigrationStatus();
        assertFalse(inProgress);
    }

    function test_CompleteMigrationWhenNotLastOperator() external whenInputsAreValid {
        // It should emit MigrationDataPullCompleted.
        // It should increment version.
        uint8[] memory operatorSlots = _getSortedOperatorSlots(2); // Two operators
        
        vm.prank(OWNER);
        cluster.startMigration(operatorSlots, 1); // Different replication strategy
        
        uint128 versionBefore = cluster.version();
        
        vm.expectEmit(true, true, true, true);
        emit MigrationDataPullCompleted(1, address(uint160(0x1001)), versionBefore + 1);
        
        vm.prank(address(uint160(0x1001))); // First operator completes (not last)
        cluster.completeMigration(1);
        
        assertEq(cluster.version(), versionBefore + 1);
        (,,bool inProgress) = cluster.getMigrationStatus();
        assertTrue(inProgress); // Still in progress
    }

    function test_AbortMigrationWhenCallerIsNotOwner() external {
        // It should revert with OwnableUnauthorizedAccount.
        uint8[] memory operatorSlots = _getSortedOperatorSlots(1);
        
        vm.prank(OWNER);
        cluster.startMigration(operatorSlots, 0);
        
        vm.expectRevert(abi.encodeWithSelector(OwnableUpgradeable.OwnableUnauthorizedAccount.selector, NON_OWNER));
        vm.prank(NON_OWNER);
        cluster.abortMigration(1);
    }

    function test_AbortMigrationWhenNoMigrationIsInProgress() external {
        // It should revert with NoMigrationInProgress.
        vm.expectRevert(Cluster.NoMigrationInProgress.selector);
        vm.prank(OWNER);
        cluster.abortMigration(1);
    }

    function test_AbortMigrationWhenMigrationIdIsWrong() external {
        // It should revert with WrongMigrationId.
        uint8[] memory operatorSlots = _getSortedOperatorSlots(1);
        
        vm.prank(OWNER);
        cluster.startMigration(operatorSlots, 0);
        
        vm.expectRevert(Cluster.WrongMigrationId.selector);
        vm.prank(OWNER);
        cluster.abortMigration(999); // Wrong migration ID
    }

    function test_AbortMigrationWhenInputsAreValid() external {
        // It should clear all pulling states.
        // It should reset pullingCount to 0.
        // It should decrement keyspaceVersion.
        // It should emit MigrationAborted.
        // It should increment version.
        uint8[] memory operatorSlots = _getSortedOperatorSlots(1); // Different from current
        
        vm.prank(OWNER);
        cluster.startMigration(operatorSlots, 1); // Different replication strategy
        
        uint128 versionBefore = cluster.version();
        uint64 keyspaceVersionBefore = cluster.keyspaceVersion();
        
        vm.expectEmit(true, true, true, true);
        emit MigrationAborted(1, versionBefore + 1);
        
        vm.prank(OWNER);
        cluster.abortMigration(1);
        
        assertEq(cluster.version(), versionBefore + 1);
        assertEq(cluster.keyspaceVersion(), keyspaceVersionBefore - 1);
        
        (,,bool inProgress) = cluster.getMigrationStatus();
        assertFalse(inProgress);
        
        uint8[] memory pullingOps = cluster.getPullingOperators();
        assertEq(pullingOps.length, 0);
    }

    function test_UpdateSettingsWhenCallerIsNotOwner() external {
        // It should revert with OwnableUnauthorizedAccount.
        Settings memory newSettings = Settings({
            maxOperatorDataBytes: 2048,
            minOperators: 1
        });
        
        vm.expectRevert(abi.encodeWithSelector(OwnableUpgradeable.OwnableUnauthorizedAccount.selector, NON_OWNER));
        vm.prank(NON_OWNER);
        cluster.updateSettings(newSettings);
    }

    function test_UpdateSettingsWhenMinOperatorsExceedsCurrentOperatorCount() external {
        // It should revert with InsufficientOperators.
        Settings memory strictSettings = Settings({
            maxOperatorDataBytes: 4096,
            minOperators: 10 // More than current operator count of 2
        });
        
        vm.expectRevert(Cluster.InsufficientOperators.selector);
        vm.prank(OWNER);
        cluster.updateSettings(strictSettings);
    }

    function test_UpdateSettingsWhenInputsAreValid() external {
        // It should update settings.
        // It should emit SettingsUpdated.
        // It should increment version.
        Settings memory newSettings = Settings({
            maxOperatorDataBytes: 2048,
            minOperators: 1
        });
        
        uint128 versionBefore = cluster.version();
        
        vm.expectEmit(true, true, true, true);
        emit SettingsUpdated(newSettings, OWNER, versionBefore + 1);
        
        vm.prank(OWNER);
        cluster.updateSettings(newSettings);
        
        assertEq(cluster.version(), versionBefore + 1);
        
        // Verify settings were updated
        ClusterView memory clusterView = cluster.getView();
        assertEq(clusterView.settings.maxOperatorDataBytes, newSettings.maxOperatorDataBytes);
        assertEq(clusterView.settings.minOperators, newSettings.minOperators);
    }

    function test_GetAllOperatorsShouldReturnSortedOperatorAddresses() external view {
        // It should return sorted operator addresses.
        address[] memory operators = cluster.getAllOperators();
        
        assertEq(operators.length, 2);
        
        // Operators should be sorted
        assertTrue(operators[0] < operators[1]);
        
        // Should contain our test operators
        bool foundFirst = false;
        bool foundSecond = false;
        for (uint256 i = 0; i < operators.length; i++) {
            if (operators[i] == address(uint160(0x1001))) foundFirst = true;
            if (operators[i] == address(uint160(0x1002))) foundSecond = true;
        }
        assertTrue(foundFirst && foundSecond);
    }

    function test_GetCurrentKeyspaceShouldReturnCurrentKeyspaceMembersAndReplicationStrategy() external view {
        // It should return current keyspace members and replication strategy.
        (uint8[] memory members, uint8 replicationStrategy) = cluster.getCurrentKeyspace();
        
        assertEq(members.length, 2);
        assertEq(replicationStrategy, 0);
        
        // Should contain operator slots 0 and 1
        assertEq(members[0], 0);
        assertEq(members[1], 1);
    }

    function test_GetViewShouldReturnCompleteClusterState() external view {
        // It should return complete cluster state.
        ClusterView memory clusterView = cluster.getView();
        
        assertEq(clusterView.operatorCount, 2);
        assertEq(clusterView.version, 1);
        assertEq(clusterView.keyspaceVersion, 0);
        assertEq(clusterView.migrationId, 0);
        assertEq(clusterView.operators.length, 2);
        assertEq(clusterView.operatorData.length, 2);
        assertEq(clusterView.currentKeyspace.members.length, 2);
        assertEq(clusterView.currentKeyspace.replicationStrategy, 0);
        assertEq(clusterView.pullingOperators.length, 0);
        assertEq(clusterView.maintenance.slot, address(0));
        assertFalse(clusterView.maintenance.isOwnerMaintenance);
        
        // Verify settings
        assertEq(clusterView.settings.maxOperatorDataBytes, _defaultSettings().maxOperatorDataBytes);
        assertEq(clusterView.settings.minOperators, _defaultSettings().minOperators);
    }

    function test_Exposed_validateOperatorDataWhenDataIsEmpty() external {
        // It should revert with InvalidOperatorData.
        vm.expectRevert(Cluster.InvalidOperatorData.selector);
        cluster.exposed_validateOperatorData("");
    }

    function test_Exposed_validateOperatorDataWhenDataExceedsMaxOperatorDataBytes() external {
        // It should revert with InvalidOperatorData.
        bytes memory oversizedData = new bytes(4097); // Exceeds _defaultSettings().maxOperatorDataBytes
        vm.expectRevert(Cluster.InvalidOperatorData.selector);
        cluster.exposed_validateOperatorData(oversizedData);
    }

    function test_Exposed_validateOperatorDataWhenDataIsValid() external view {
        // It should not revert.
        bytes memory validData = "valid operator data";
        cluster.exposed_validateOperatorData(validData); // Should not revert
    }

    function test_Exposed_findAvailableSlotWhenAllSlotsAreOccupied() external {
        // It should revert with TooManyOperators.
        // Create a cluster at max capacity
        NodeOperator[] memory maxOperators = new NodeOperator[](255);
        for (uint256 i = 0; i < 255; i++) {
            maxOperators[i] = NodeOperator({
                addr: address(uint160(0x4000 + i)),
                data: abi.encodePacked("slot_op", i),
                maintenance: false
            });
        }
        
        ClusterHarness implementation = new ClusterHarness();
        bytes memory initData = abi.encodeCall(Cluster.initialize, (_defaultSettings(), maxOperators));
        
        vm.prank(OWNER);
        ERC1967Proxy proxy = new ERC1967Proxy(address(implementation), initData);
        ClusterHarness maxCluster = ClusterHarness(address(proxy));
        
        vm.expectRevert(Cluster.TooManyOperators.selector);
        maxCluster.exposed_findAvailableSlot();
    }

    function test_Exposed_findAvailableSlotWhenSlotsAreAvailable() external view {
        // It should return the first available slot.
        uint8 availableSlot = cluster.exposed_findAvailableSlot();
        assertEq(availableSlot, 2); // Slots 0,1 are occupied, so slot 2 should be available
    }

    function test_Exposed_isSlotInKeyspaceWhenSlotIsInKeyspace() external view {
        // It should return true.
        bool isInKeyspace = cluster.exposed_isSlotInKeyspace(0, 0); // Slot 0 in keyspace 0
        assertTrue(isInKeyspace);
    }

    function test_Exposed_isSlotInKeyspaceWhenSlotIsNotInKeyspace() external view {
        // It should return false.
        bool isInKeyspace = cluster.exposed_isSlotInKeyspace(99, 0); // Slot 99 not in keyspace 0
        assertFalse(isInKeyspace);
    }
}
