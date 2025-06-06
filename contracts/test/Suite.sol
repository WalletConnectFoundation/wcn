// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "../dependencies/forge-std/src/Test.sol";
import {Vm} from "../dependencies/forge-std/src/Vm.sol";
import "../dependencies/forge-std/src/console.sol";

import "../src/Cluster.sol";

import "../dependencies/openzeppelin-contracts/utils/Strings.sol";

uint256 constant OWNER = 12345;
uint256 constant NEW_OWNER = 56789;
uint256 constant ANYONE = 9000;
uint256 constant OPERATOR = 1;

bytes constant DEFAULT_OPERATOR_DATA = "Some operator specific data";
uint16 constant MAX_OPERATOR_DATA_BYTES = 4096;

contract ClusterTest is Test {
    using Bitmask for uint256;
    using Strings for uint256;

    Cluster cluster;
    ClusterView clusterView;

    constructor() {
        newCluster();
    }

    function test_bitmask() public pure {
        uint256 bitmask;

        for (uint256 i = 0; i < 256; i++) {
            bitmask = Bitmask.fill(uint8(i));
            assertEq(bitmask.count1(), i);

            if (i == 0) {
                assertEq(bitmask.highest1(), 0);
            } else {
                assertEq(bitmask.highest1(), i - 1);
                assertEq(bitmask.is1(uint8(i - 1)), true);
                if (i != 256) {
                    assertEq(bitmask.is1(uint8(i)), false);
                }
            }
        }

        bitmask = Bitmask.fill(5);
        assertEq(bitmask, 0x1F);

        bitmask = bitmask.set1(5);
        bitmask = bitmask.set1(6);
        bitmask = bitmask.set1(7);
        assertEq(bitmask, 0xFF);
        assertEq(bitmask.count1(), 8);
        assertEq(bitmask.highest1(), 7);

        bitmask = bitmask.set0(5);
        assertEq(bitmask, 0xDF);
        assertEq(bitmask.count1(), 7);
        assertEq(bitmask.highest1(), 7);

        assertEq(parseBitmask("1010"), 10);

        assert(parseBitmask("1010").isSubsetOf(parseBitmask("1110")));
        assert(!parseBitmask("1010").isSubsetOf(parseBitmask("1101")));
    }

    // contructor

    function test_canNotCreateClusterWithTooManyOperators() public {
        vm.expectRevert();
        newCluster(257);
    }

    function test_canCreateClusterWithMaxNumberOfOperators() public {
        newCluster(256);
    }

    function test_clusterContainsInitialNodeOperators() public view {
        assertNodeOperatorSlotsLength(5);
        for (uint256 i = 0; i < 5; i++) {
            assertNodeOperatorSlot(i, newNodeOperator(i + 1));
        }
    }

    function test_clusterContainsInitialKeyspace() public view {
        assertKeyspace(0, newKeyspace("11111"));
        assertKeyspace(1, newKeyspace("0"));
    }

    function test_clusterInitialVersionIs0() public view {
        assertVersion(0);
    }

    function test_clusterInitialKeyspaceVersionIs0() public view {
        assertKeyspaceVersion(0);
    }

    // addNodeOperator
    
    function test_anyoneCanNotAddNodeOperator() public {
        expectRevert("not the owner");
        addNodeOperator(ANYONE, 5, newNodeOperator(6));
    }

    function test_operatorCanNotAddAnotherNodeOperator() public {
        expectRevert("not the owner");
        addNodeOperator(ANYONE, 5, newNodeOperator(6));
    }

    function test_ownerCanAddNodeOperator() public {
        addNodeOperator(OWNER, 5, newNodeOperator(6));
    }

    function test_addNodeOperatorBumpsVersion() public {
        addNodeOperator(OWNER, 5, newNodeOperator(6));
        assertVersion(1);
    }

    function test_addNodeOperatorDoesNotBumpKeyspaceVersion() public {
        addNodeOperator(OWNER, 5, newNodeOperator(6));
        assertKeyspaceVersion(0);
    }

    function test_addNodeOperatorEmitsEventNodeOperatorAdded() public {
        NodeOperator memory operator = newNodeOperator(6);
        vm.expectEmit();
        emit NodeOperatorAdded(5, operator, 1);
        addNodeOperator(OWNER, 5, operator);
    }

    // updateNodeOperator

    function test_anyoneCanNotUpdateNodeOperator() public {
        expectRevert("unauthorized");
        updateNodeOperator(ANYONE, newNodeOperator(1, "new data"));
    }

    function test_ownerCanUpdateNodeOperator() public {
        updateNodeOperator(OWNER, newNodeOperator(1, "new data"));
    }

    function test_operatorCanNotUpdateAnotherNodeOperator() public {
        expectRevert("unauthorized");
        updateNodeOperator(1, newNodeOperator(2, "new data"));
    }

    function test_operatorCanUpdateItself() public {
        updateNodeOperator(1, newNodeOperator(1, "new data"));
    }

    function test_updateNodeOperatorDoesUpdateTheData() public {
        NodeOperator memory operator = newNodeOperator(1, "new data");
        updateNodeOperator(1, operator);
        assertNodeOperatorSlot(0, operator);
    }

    function test_updateNodeOperatorBumpsVersion() public {
        updateNodeOperator(1, newNodeOperator(1, "new data"));
        assertVersion(1);
    }

    function test_updateNodeOperatorDoesNotBumpKeyspaceVersion() public {
        updateNodeOperator(1, newNodeOperator(1, "new data"));
        assertKeyspaceVersion(0);
    }

    function test_updateNodeOperatorEmitsEventNodeOperatorUpdated() public {
        NodeOperator memory operator = newNodeOperator(1, "new data");
        vm.expectEmit();
        emit NodeOperatorUpdated(operator, 1);
        updateNodeOperator(1, operator);
    }

    // removeNodeOperator

    function test_anyoneCanNotRemoveNodeOperator() public {
        expectRevert("not the owner");
        removeNodeOperator(ANYONE, 1);
    }

    function test_operatorCanNotRemoveAnotherNodeOperator() public {
        expectRevert("not the owner");
        removeNodeOperator(1, 2);
    }

    function test_operatorCanNotRemoveItself() public {
        expectRevert("not the owner");
        removeNodeOperator(1, 1);
    }

    function test_ownerCanRemoveNodeOperator() public {
        addNodeOperator(OWNER, 5, newNodeOperator(6));
        removeNodeOperator(OWNER, 6);
    }

    function test_canNotRemoveNodeOperatorInKeyspace() public {
        expectRevert("in keyspace");
        removeNodeOperator(OWNER, 1);
    }

    function test_removeNodeOperatorClearsTheSlot() public {
        addNodeOperator(OWNER, 5, newNodeOperator(6));
        assertNodeOperatorSlotsLength(6);
        removeNodeOperator(OWNER, 6);
        assertNodeOperatorSlotsLength(5);
    }

    function test_removeNodeOperatorBumpsVersion() public {
        addNodeOperator(OWNER, 5, newNodeOperator(6));
        assertVersion(1);
        removeNodeOperator(OWNER, 6);
        assertVersion(2);
    }

    function test_removeNodeOperatorDoesNotBumpKeyspaceVersion() public {
        addNodeOperator(OWNER, 5, newNodeOperator(6));
        removeNodeOperator(OWNER, 6);
        assertKeyspaceVersion(0);
    }

    function test_removeNodeOperatorEmitsEventNodeOperatorRemoved() public {
        addNodeOperator(OWNER, 5, newNodeOperator(6));
        vm.expectEmit();
        emit NodeOperatorRemoved(vm.addr(6), 2);
        removeNodeOperator(OWNER, 6);
    }

    // startMigration

    function test_anyoneCanNotStartMigration() public {
        expectRevert("not the owner");
        startMigration(ANYONE, newKeyspace("01111"));
    }

    function test_operatorCanNotStartMigration() public {
        expectRevert("not the owner");
        startMigration(OPERATOR, newKeyspace("01111"));
        }

    function test_ownerCanStartMigration() public {
        startMigration(OWNER, newKeyspace("01111"));
    }

    function test_canNotStartMigrationWhenMaintenanceInProgress() public {
        startMaintenance(1);
        expectRevert("maintenance in progress");
        startMigration(OWNER, newKeyspace("01111"));
    }

    function test_canNotStartMigrationWhenMigrationInProgress() public {
        startMigration(OWNER, newKeyspace("01111"));
        expectRevert("migration in progress");
        startMigration(OWNER, newKeyspace("11111"));
    }

    function test_startMigrationBumpsVersion() public {
        startMigration(OWNER, newKeyspace("01111"));
        assertVersion(1);
    }

    function test_startMigrationBumpsKeyspaceVersion() public {
        startMigration(OWNER, newKeyspace("01111"));
        assertKeyspaceVersion(1);
    }

    function test_startMigrationEmitsMigrationStartedEvent() public {
        Keyspace memory keyspace = newKeyspace("01111");
        vm.expectEmit();
        emit MigrationStarted(1, keyspace, 1, 1);
        startMigration(OWNER, keyspace);
    }

    function test_startMigrationInitializesMigration() public {
        addNodeOperator(OWNER, 5, newNodeOperator(6));
        addNodeOperator(OWNER, 6, newNodeOperator(7));
        startMigration(OWNER, newKeyspace("1111101"));
        assertMigration(1, "1111101");
    }

    function test_startMigrationPopulatesMigrationKeyspace() public {
        addNodeOperator(OWNER, 5, newNodeOperator(6));
        addNodeOperator(OWNER, 6, newNodeOperator(7));
        Keyspace memory keyspace = newKeyspace("1111101");
        startMigration(OWNER, keyspace);
        assertKeyspace(1, keyspace);
    }

    // completeMigration

    function test_anyoneCanNotCompleteMigration() public {
        startMigration(OWNER, newKeyspace("01111"));
        expectRevert("unknown operator");
        completeMigration(ANYONE, 1);
    }

    function test_ownerCanNotCompleteMigration() public {
        startMigration(OWNER, newKeyspace("01111"));
        expectRevert("unknown operator");
        completeMigration(OWNER, 1);
    }

    function test_operatorCanNotCompleteNonExistentMigration() public {
        expectRevert("no migration");
        completeMigration(OPERATOR, 1);
    }

    function test_operatorCanCompleteMigration() public {
        startMigration(OWNER, newKeyspace("01111"));
        completeMigration(OPERATOR, 1);
    }

    function test_canNotCompleteMigrationTwice() public {
        startMigration(OWNER, newKeyspace("01111"));
        completeMigration(OPERATOR, 1);
        expectRevert("not pulling");
        completeMigration(OPERATOR, 1);
    }

    function test_completeMigrationBumpsVersion() public {
        startMigration(OWNER, newKeyspace("01111"));
        completeMigration(OPERATOR, 1);
        assertVersion(2);
    }

    function test_completeMigrationRemovesOperatorFromPullingOperators() public {
        startMigration(OWNER, newKeyspace("11110"));
        assertMigration(1, "11110");
        completeMigration(5, 1);
        assertMigration(1, "01110");
        completeMigration(3, 1);
        assertMigration(1, "01010");
        completeMigration(2, 1);
        assertMigration(1, "01000");
        completeMigration(4, 1);
        assertMigration(1, "00000");
    }

    function test_completeMigrationDoesNotBumpKeyspaceVersionIfNotCompleted() public {
        assertKeyspaceVersion(0);
        startMigration(OWNER, newKeyspace("01111"));
        assertKeyspaceVersion(1);
        completeMigration(1, 1);
        assertKeyspaceVersion(1);
    }

    function test_completeMigrationDoesNotBumpKeyspaceVersionIfCompleted() public {
        assertKeyspaceVersion(0);
        startMigration(OWNER, newKeyspace("01111"));
        assertKeyspaceVersion(1);
        for (uint256 i = 0; i < 4; i++) {
            completeMigration(i + 1, 1);
        }
        assertKeyspaceVersion(1);
    }

    function test_completeMigrationEmitsMigrationDataPullCompletedEventIfNotCompleted() public {
        startMigration(OWNER, newKeyspace("01111"));
        vm.expectEmit();
        emit MigrationDataPullCompleted(1, vm.addr(1), 2);
        completeMigration(1, 1);
    }

    function test_completeMigrationEmitsMigrationCompletedEventIfCompleted() public {
        startMigration(OWNER, newKeyspace("01111"));
        completeMigration(1, 1);
        completeMigration(2, 1);
        completeMigration(3, 1);

        vm.expectEmit();
        emit MigrationCompleted(1, vm.addr(4), 5);
        completeMigration(4, 1);
    }

    // abortMigration

    function test_anyoneCanNotAbortMigration() public {
        startMigration(OWNER, newKeyspace("01111"));
        expectRevert("not the owner");
        abortMigration(ANYONE, 1);
    }

    function test_operatorCanNotAbortMigration() public {
        startMigration(OWNER, newKeyspace("01111"));
        expectRevert("not the owner");
        abortMigration(OPERATOR, 1);
    }

    function test_ownerCanAbortMigration() public {
        startMigration(OWNER, newKeyspace("01111"));
        abortMigration(OWNER, 1);
    }

    function test_canNotAbortNonExistentMigration() public {
        expectRevert("no migration");
        abortMigration(OWNER, 1);
    }

    function test_abortMigrationBumpsVersion() public {
        startMigration(OWNER, newKeyspace("01111"));
        abortMigration(OWNER, 1);
        assertVersion(2);
    }

    function test_abortMigrationRevertsKeyspaceVersion() public {
        assertKeyspaceVersion(0);
        startMigration(OWNER, newKeyspace("01111"));
        assertKeyspaceVersion(1);
        abortMigration(OWNER, 1);
        assertKeyspaceVersion(0);
    }

    function test_abortMigrationClearsPullingOperators() public {
        startMigration(OWNER, newKeyspace("01111"));
        abortMigration(OWNER, 1);
        assertMigration(1, "00000");
    }

    function test_abortMigrationEmitsMigrationAbortedEvent() public {
        startMigration(OWNER, newKeyspace("01111"));
        vm.expectEmit();
        emit MigrationAborted(1, 2);
        abortMigration(OWNER, 1);
    }

    // startMaintenance 

    function test_anyoneCanNotStartMaintenance() public {
        expectRevert("unauthorized");
        startMaintenance(ANYONE);
    }

    function test_ownerCanStartMaintenance() public {
        startMaintenance(OWNER);
    }

    function test_operatorCanStartMaintenance() public {
        startMaintenance(OPERATOR);
    }

    function test_canNotStartMaintenanceWhenMaintenanceInProgress() public {
        startMaintenance(1);
        expectRevert("maintenance in progress");
        startMaintenance(2);
    }

    function test_canNotStartMaintenanceWhenMigrationInProgress() public {
        startMigration(OWNER, newKeyspace("01111"));
        expectRevert("migration in progress");
        startMaintenance(1);
    }

    function test_startMaintenanceBumpsVersion() public {
        startMaintenance(1);
        assertVersion(1);
    }

    function test_startMaintenanceDoesNotBumpKeyspaceVersion() public {
        startMaintenance(1);
        assertKeyspaceVersion(0);
    }

    function test_startMaintenanceUpdatesMaintenance() public {
        startMaintenance(1);
        assertMaintenance(1);
    }

    function test_startMaintenanceEmitsMaintenanceStartedEvent() public {
        vm.expectEmit();
        emit MaintenanceStarted(vm.addr(1), 1);
        startMaintenance(1);
    }

    // finishMaintenance

    function test_anyoneCanNotFinishMaintenance() public {
        startMaintenance(1);
        expectRevert("unauthorized");
        finishMaintenance(ANYONE);
    }

    function test_anotherOperatorCanNotFinishMaintenance() public {
        startMaintenance(2);
        expectRevert("unauthorized");
        finishMaintenance(1);
    }

    function test_ownerCanFinishMaintenance() public {
        startMaintenance(1);
        finishMaintenance(OWNER);
    }

    function test_operatorCanFinishMaintenance() public {
        startMaintenance(OPERATOR);
        finishMaintenance(OPERATOR);
    }

    function test_canNotFinishNonExistentMaintenance() public {
        expectRevert("no maintenance");
        finishMaintenance(1);
    }

    function test_finishMaintenanceBumpsVersion() public {
        startMaintenance(1);
        finishMaintenance(1);
        assertVersion(2);
    }

    function test_finishMaintenanceDoesNotBumpKeyspaceVersion() public {
        startMaintenance(1);
        finishMaintenance(1);
        assertKeyspaceVersion(0);
    }

    function test_finishMaintenanceFreesMaintenanceSlot() public {
        startMaintenance(1);
        finishMaintenance(1);
        assertNoMaintenance();
    }
    
    function test_finishMaintenanceEmitsMaintenanceFinishedEvent() public {
        startMaintenance(1);
        vm.expectEmit();
        emit MaintenanceFinished(2);
        finishMaintenance(1);
    }
    
    // updateSettings

    function test_anyoneCanNotUpdateSettings() public {
        expectRevert("not the owner");
        updateSettings(ANYONE, Settings({ maxOperatorDataBytes: 100 }));
    }

    function test_operatorCanNotUpdateSettings() public {
        expectRevert("not the owner");
        updateSettings(OPERATOR, Settings({ maxOperatorDataBytes: 100 }));
    }

    function test_ownerCanUpdateSettings() public {
        updateSettings(OWNER, Settings({ maxOperatorDataBytes: 100 }));
    }

    function test_updateSettingsUpdatesMaxOperatorDataBytes() public {
        updateSettings(OWNER, Settings({ maxOperatorDataBytes: 5 }));
        expectRevert("operator data too large");
        addNodeOperator(OWNER, 5, newNodeOperator(10, "123456"));
    }

    function test_updateSettingsBumpsVersion() public {
        updateSettings(OWNER, Settings({ maxOperatorDataBytes: 100 }));
        assertVersion(1);
    }

    function test_updateSettingsDoesNotBumpKeyspaceVersion() public {
        updateSettings(OWNER, Settings({ maxOperatorDataBytes: 100 }));
        assertKeyspaceVersion(0);
    }

    // transferOwnership

    function test_anyoneCanNotTransferOwnership() public {
        expectRevert("not the owner");
        transferOwnership(ANYONE, ANYONE);
    }

    function test_operatorCanNotTransferOwnership() public {
        expectRevert("not the owner");
        transferOwnership(OPERATOR, OPERATOR);
    }

    function test_ownerCanTransferOwnership() public {
        transferOwnership(OWNER, NEW_OWNER);
    }

    function test_transferOwnershipChangesOwner() public {
        transferOwnership(OWNER, NEW_OWNER);
        startMigration(NEW_OWNER, newKeyspace("01111"));
    }

    // full lifecycle

    function test_fullClusterLifecycle() public {
        updateNodeOperator(1, newNodeOperator(1, "operator1"));
        startMaintenance(2);
        updateNodeOperator(3, newNodeOperator(3, "operator3"));
        finishMaintenance(2);

        addNodeOperator(OWNER, 5, newNodeOperator(6, "operator6"));
        addNodeOperator(OWNER, 6, newNodeOperator(7, "operator7"));
        addNodeOperator(OWNER, 7, newNodeOperator(8, "operator8"));
        startMigration(OWNER, newKeyspace("11111111"));
        updateNodeOperator(1, newNodeOperator(1, "operator1"));
        for (uint256 i = 0; i < 8; i++) {
            completeMigration(i + 1, 1);
        }
        updateNodeOperator(3, newNodeOperator(3, "operator1"));
        startMaintenance(7);
        updateNodeOperator(7, newNodeOperator(7, "operator1"));
        finishMaintenance(OWNER);

        startMigration(OWNER, newKeyspace("10111111"));
        for (uint256 i = 0; i < 8; i++) {
            if (i != 6) {
                completeMigration(i + 1, 2);
            }
        }

        addNodeOperator(OWNER, 8, newNodeOperator(9, "operator9"));
        startMigration(OWNER, newKeyspace("110111111"));
        for (uint256 i = 0; i < 8; i++) {
            if (i != 6) {
                completeMigration(i + 1, 3);
            }
        }
        abortMigration(OWNER, 3);

        transferOwnership(OWNER, NEW_OWNER);
        updateSettings(NEW_OWNER, Settings({ maxOperatorDataBytes: 1024 }));

        assertKeyspaceVersion(2);
    }

    // internal

    function newCluster() internal {   
        newCluster(5);
    }

    function newCluster(uint256 operatorsCount) internal {   
        newCluster(Settings({ maxOperatorDataBytes: MAX_OPERATOR_DATA_BYTES }), operatorsCount);
    }

    function newCluster(Settings memory settings, uint256 operatorsCount) internal {
        setCaller(OWNER);

        NodeOperator[] memory operators = new NodeOperator[](operatorsCount);
        for (uint256 i = 0; i < operatorsCount; i++) {
            operators[i] = newNodeOperator(i + 1);
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

    function assertNodeOperatorSlotsLength(uint256 expected) internal view {
        assertEq(clusterView.nodeOperatorSlots.length, expected);
    }

    function assertNodeOperatorSlot(uint256 idx, NodeOperator memory slot) internal view {
        assertEq(clusterView.nodeOperatorSlots[idx].addr, slot.addr);
        assertEq(clusterView.nodeOperatorSlots[idx].data, slot.data);
    }

    function assertVersion(uint128 expectedVersion) internal view {
        assertEq(clusterView.version, expectedVersion);
    }

    function assertKeyspaceVersion(uint64 expectedVersion) internal view {
        assertEq(clusterView.keyspaceVersion, expectedVersion);
    }

    function assertKeyspace(uint256 idx, Keyspace memory expected) internal view {
        assertEq(clusterView.keyspaces[idx].operatorsBitmask, expected.operatorsBitmask);
        assertEq(clusterView.keyspaces[idx].replicationStrategy, expected.replicationStrategy);
    }

    function assertMigration(uint64 id, string memory pullingOperatorsBitmaskStr) internal view {
        uint256 pullingOperatorsBitmask = parseBitmask(pullingOperatorsBitmaskStr);

        assertEq(clusterView.migration.id, id);
        assertEq(clusterView.migration.pullingOperatorsBitmask, pullingOperatorsBitmask);
    }

    function assertMaintenance(uint256 operator) internal view {
        assertEq(clusterView.maintenance.slot, vm.addr(operator));
    }

    function assertNoMaintenance() internal view {
        assertEq(clusterView.maintenance.slot, address(0));
    }

    function startMigration(uint256 caller, Keyspace memory keyspace) internal {
        setCaller(caller);
        cluster.startMigration(keyspace);
        updateClusterView();
    }

    function completeMigration(uint256 caller, uint64 id) internal {
        setCaller(caller);
        cluster.completeMigration(id);
        updateClusterView();
    }

    function abortMigration(uint256 caller, uint64 id) internal {
        setCaller(caller);
        cluster.abortMigration(id);
        updateClusterView();
    }

    function startMaintenance(uint256 caller) internal {
        setCaller(caller);
        cluster.startMaintenance();
        updateClusterView();
    }

    function finishMaintenance(uint256 caller) internal {
        setCaller(caller);
        cluster.finishMaintenance();
        updateClusterView();
    }

    function addNodeOperator(uint256 caller, uint8 idx, NodeOperator memory operator) internal {
        setCaller(caller);
        cluster.addNodeOperator(idx, operator);
        updateClusterView();
    }

    function updateNodeOperator(uint256 caller, NodeOperator memory operator) internal {
        setCaller(caller);
        cluster.updateNodeOperator(operator);
        updateClusterView();
    }

    function removeNodeOperator(uint256 caller, uint256 privateKey) internal {
        setCaller(caller);
        cluster.removeNodeOperator(vm.addr(privateKey));
        updateClusterView();
    }

    function updateSettings(uint256 caller, Settings memory settings) internal {
        setCaller(caller);
        cluster.updateSettings(settings);
        updateClusterView();
    }

    function transferOwnership(uint256 caller, uint256 newOwner) internal {
        setCaller(caller);
        cluster.transferOwnership(vm.addr(newOwner));
        updateClusterView();
    }

    // function emptyNodeOperatorSlot() internal pure returns (NodeOperator memory) {
    //     NodeOperator memory operator;
    //     return operator;
    // }

    function newNodeOperator(uint256 privateKey) internal pure returns (NodeOperator memory) {
        return newNodeOperator(privateKey, abi.encodePacked("operator ", privateKey.toString()));
    }

    function newNodeOperator(uint256 privateKey, bytes memory data) internal pure returns (NodeOperator memory) {
        return NodeOperator({ addr: vm.addr(privateKey), data: data });
    }
}

function newKeyspace(string memory operatorsBitmaskStr) pure returns (Keyspace memory) {
    return newKeyspace(operatorsBitmaskStr, 0);
}

function newKeyspace(string memory operatorsBitmaskStr, uint8 replicationStrategy) pure returns (Keyspace memory) {
    return Keyspace({
        operatorsBitmask: parseBitmask(operatorsBitmaskStr),
        replicationStrategy: replicationStrategy
    });
}

function parseBitmask(string memory binary) pure returns (uint256 result) {
    bytes memory b = bytes(binary);
    for (uint256 i = 0; i < b.length; i++) {
        result <<= 1;
        bytes1 c = b[i];
        require(c == "0" || c == "1", "Invalid binary char");
        if (c == "1") {
            result |= 1;
        }
    }
}
