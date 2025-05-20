// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "../dependencies/forge-std-1.9.7/src/Test.sol";
import {Vm} from "../dependencies/forge-std-1.9.7/src/Vm.sol";
import "../dependencies/forge-std-1.9.7/src/console.sol";

import "../src/Cluster.sol";

uint256 constant OWNER = 12345;
uint256 constant NEW_OWNER = 56789;
uint256 constant ANYONE = 9000;
uint256 constant OPERATOR = 1;

bytes constant DEFAULT_OPERATOR_DATA = "Some operator specific data";
uint16 constant MAX_OPERATOR_DATA_BYTES = 4096;

contract ClusterTest is Test {
    using Bitmask for uint256;

    Cluster cluster;
    ClusterView clusterView;

    constructor() {
        newCluster(vm);
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
    }

    // contructor

    function test_canNotCreateClusterWithTooManyOperators() public {
        vm.expectRevert();
        newCluster(vm, 257);
    }

    function test_canCreateClusterWithMaxNumberOfOperators() public {
        newCluster(vm, 256);
    }

    function test_clusterContainsInitialOperatorsInPrimaryKeyspace() public view {
        assertKeyspaceSlotsCount(clusterView.primaryKeyspace, 5);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 0, 1);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 1, 2);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 2, 3);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 3, 4);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 4, 5);
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
        startMigration(ANYONE, newMigration().clear(0));
    }

    function test_operatorCanNotStartMigration() public {
        expectRevert("not the owner");
        startMigration(OPERATOR, newMigration().clear(0));
    }

    function test_ownerCanStartMigration() public {
        startMigration(OWNER, newMigration().clear(0));
    }

    function test_canNotStartMigrationWhenMaintenanceInProgress() public {
        startMaintenance(1, 0);
        expectRevert("maintenance in progress");
        startMigration(OWNER, newMigration().clear(0));
    }

    function test_startMigrationBumpsVersion() public {
        startMigration(OWNER, newMigration().clear(0));
        assertVersion(1);
    }

    function test_startMigrationBumpsKeyspaceVersion() public {
        startMigration(OWNER, newMigration().clear(0));
        assertKeyspaceVersion(1);
    }

    function test_startMigrationEmitsMigrationStartedEvent() public {
        TestMigration memory migration = newMigration().clear(0);
        vm.expectEmit();
        emit MigrationStarted(1, migration.plan, 1);
        startMigration(OWNER, migration);
    }

    function test_startMigrationInitializesMigration() public {
        startMigration(OWNER, newMigration().set(4, 6).set(5, 7).clear(2));
        assertMigration(1, 5);
        assertMigrationPullingOperator(0);
        assertMigrationPullingOperator(1);
        assertMigrationPullingOperator(3);
        assertMigrationPullingOperator(4);
        assertMigrationPullingOperator(5);
    }

    function test_startMigrationPopulatesSecondaryKeyspace() public {
        registerNodeOperator(6, "operator6");
        registerNodeOperator(7, "operator7");
    
        startMigration(OWNER, newMigration().set(4, 6).set(5, 7).clear(2));
        assertKeyspaceSlotsCount(clusterView.secondaryKeyspace, 6);
        assertKeyspaceSlot(clusterView.secondaryKeyspace, 0, 1, "");
        assertKeyspaceSlot(clusterView.secondaryKeyspace, 1, 2, "");
        assertKeyspaceSlotEmpty(clusterView.secondaryKeyspace, 2);
        assertKeyspaceSlot(clusterView.secondaryKeyspace, 3, 4, "");
        assertKeyspaceSlot(clusterView.secondaryKeyspace, 4, 6, "operator6");
        assertKeyspaceSlot(clusterView.secondaryKeyspace, 5, 7, "operator7");
    }

    // completeMigration

    function test_anyoneCanNotCompleteMigration() public {
        startMigration(OWNER, newMigration().clear(0));
        expectRevert("wrong operator");
        completeMigration(ANYONE, 1, 0);
    }

    function test_ownerCanNotCompleteMigration() public {
        startMigration(OWNER, newMigration().clear(0));
        expectRevert("wrong operator");
        completeMigration(OWNER, 1, 0);
    }

    function test_operatorCanNotCompleteNonExistentMigration() public {
        expectRevert("no migration");
        completeMigration(OPERATOR, 1, 0);
    }

    function test_operatorCanCompleteMigration() public {
        startMigration(OWNER, newMigration().clear(1));
        completeMigration(OPERATOR, 1, 0);
    }

    function test_completeMigrationIsIdempotent() public {
        startMigration(OWNER, newMigration().clear(1));
        completeMigration(OPERATOR, 1, 0);
        completeMigration(OPERATOR, 1, 0);
    }

    function test_completeMigrationBumpsVersion() public {
        startMigration(OWNER, newMigration().clear(1));
        completeMigration(OPERATOR, 1, 0);
        assertVersion(2);
    }

    function test_completeMigrationDoesNotUpdatePrimaryKeyspaceIfNotCompleted() public {
        startMigration(OWNER, newMigration().set(5, 6));
        completeMigration(OPERATOR, 1, 0);
        assertKeyspaceSlotsCount(clusterView.primaryKeyspace, 5);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 0, 1);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 1, 2);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 2, 3);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 3, 4);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 4, 5);
    }

    function test_completeMigrationUpdatesOperatorsIfCompleted() public {
        registerNodeOperator(6, "operator6");
        startMigration(OWNER, newMigration().set(4, 6));
        completeMigration(1, 1, 0);
        completeMigration(2, 1, 1);
        completeMigration(3, 1, 2);
        completeMigration(4, 1, 3);
        completeMigration(6, 1, 4);
        assertKeyspaceSlotsCount(clusterView.primaryKeyspace, 5);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 0, 1);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 1, 2);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 2, 3);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 3, 4);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 4, 6, "operator6");
    }

    function test_completeMigrationDeletesMigrationIfCompleted() public {
        startMigration(OWNER, newMigration().clear(3).clear(4));
        completeMigration(1, 1, 0);
        completeMigration(2, 1, 1);
        completeMigration(3, 1, 2);
        assertNoMigration();
    }

    function test_completeMigrationRemovesPullingOperatorBitIfNotCompleted() public {
        startMigration(OWNER, newMigration().set(2, 10));
        completeMigration(1, 1, 0);
        assert(clusterView.migration.pullingOperatorsBitmask.is0(0));
    }

    function test_completeMigrationDoesNotBumpKeyspaceVersionIfNotCompleted() public {
        startMigration(OWNER, newMigration().set(2, 10));
        completeMigration(1, 1, 0);
        assertKeyspaceVersion(1);
    }

    function test_completeMigrationDoesNotBumpKeyspaceVersionIfCompleted() public {
        startMigration(OWNER, newMigration().clear(3).clear(4));
        completeMigration(1, 1, 0);
        completeMigration(2, 1, 1);
        completeMigration(3, 1, 2);
        assertKeyspaceVersion(1);
    }

    function test_completeMigrationEmitsMigrationDataPullCompletedEventIfNotCompleted() public {
        startMigration(OWNER, newMigration().set(2, 10));
        vm.expectEmit();
        emit MigrationDataPullCompleted(1, vm.addr(1), 2);
        completeMigration(1, 1, 0);
    }

    function test_completeMigrationEmitsMigrationCompletedEventIfCompleted() public {
        startMigration(OWNER, newMigration().clear(3).clear(4));
        completeMigration(1, 1, 0);
        completeMigration(2, 1, 1);
        vm.expectEmit();
        emit MigrationCompleted(1, vm.addr(3), 4);
        completeMigration(3, 1, 2);
    }

    // abortMigration

    function test_anyoneCanNotAbortMigration() public {
        startMigration(OWNER, newMigration().clear(0));
        expectRevert("not the owner");
        abortMigration(ANYONE);
    }

    function test_operatorCanNotAbortMigration() public {
        startMigration(OWNER, newMigration().clear(0));
        expectRevert("not the owner");
        abortMigration(OPERATOR);
    }

    function test_ownerCanAbortMigration() public {
        startMigration(OWNER, newMigration().clear(0));
        abortMigration(OWNER);
    }

    function test_canNotAbortNonExistentMigration() public {
        expectRevert("no migration");
        abortMigration(OWNER);
    }

    function test_abortMigrationBumpsVersion() public {
        startMigration(OWNER, newMigration().clear(0));
        abortMigration(OWNER);
        assertVersion(2);
    }

    function test_abortMigrationRevertsKeyspaceVersion() public {
        startMigration(OWNER, newMigration().clear(0));
        abortMigration(OWNER);
        assertKeyspaceVersion(0);
    }

    function test_abortMigrationDoesNotUpdatePrimaryKeyspace() public {
        startMigration(OWNER, newMigration().clear(0));
        abortMigration(OWNER);
        assertKeyspaceSlotsCount(clusterView.primaryKeyspace, 5);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 0, 1);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 1, 2);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 2, 3);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 3, 4);
        assertKeyspaceSlot(clusterView.primaryKeyspace, 4, 5);
    }

    function test_abortMigrationDeletesMigration() public {
        startMigration(OWNER, newMigration().clear(0));
        abortMigration(OWNER);
        assertNoMigration();
    }

    function test_abortMigrationEmitsMigrationAbortedEvent() public {
        startMigration(OWNER, newMigration().clear(0));
        vm.expectEmit();
        emit MigrationAborted(1, 2);
        abortMigration(OWNER);
    }

    // startMaintenance 

    function test_anyoneCanNotStartMaintenance() public {
        expectRevert("wrong operator");
        startMaintenance(ANYONE, 0);
    }

    function test_ownerCanNotStartMaintenance() public {
        expectRevert("wrong operator");
        startMaintenance(OWNER, 0);
    }

    function test_operatorCanStartMaintenance() public {
        startMaintenance(OPERATOR, 0);
    }

    function test_canNotStartMoreThanOneMaintenance() public {
        startMaintenance(1, 0);
        expectRevert("maintenance in progress");
        startMaintenance(2, 1);
    }

    function test_operatorCanNotStartMaintenanceWhenMigrationInProgress() public {
        startMigration(OWNER, newMigration().clear(1));
        expectRevert("migration in progress");
        startMaintenance(1, 0);
    }

    function test_startMaintenanceBumpsVersion() public {
        startMaintenance(1, 0);
        assertVersion(1);
    }

    function test_startMaintenanceDoesNotBumpKeyspaceVersion() public {
        startMaintenance(1, 0);
        assertKeyspaceVersion(0);
    }

    function test_startMaintenanceUpdatesMaintenance() public {
        startMaintenance(1, 0);
        assertMaintenance(1);
    }

    function test_startMaintenanceEmitsMaintenanceStartedEvent() public {
        vm.expectEmit();
        emit MaintenanceStarted(vm.addr(1), 1);
        startMaintenance(1, 0);
    }

    // completeMaintenance

    function test_anyoneCanNotCompleteMaintenance() public {
        startMaintenance(1, 0);
        expectRevert("wrong operator");
        completeMaintenance(ANYONE);
    }

    function test_anotherOperatorCanNotCompleteMaintenance() public {
        startMaintenance(2, 1);
        expectRevert("wrong operator");
        completeMaintenance(OPERATOR);
    }

    function test_ownerCanNotCompleteMaintenance() public {
        startMaintenance(1, 0);
        expectRevert("wrong operator");
        completeMaintenance(OWNER);
    }

    function test_sameOperatorCanCompleteMaintenance() public {
        startMaintenance(1, 0);
        completeMaintenance(OPERATOR);
    }

    function test_canNotCompleteNonExistentMaintenance() public {
        expectRevert("no maintenance");
        completeMaintenance(OPERATOR);
    }

    function test_completeMaintenanceBumpsVersion() public {
        startMaintenance(1, 0);
        completeMaintenance(1);
        assertVersion(2);
    }

    function test_completeMaintenanceDoesNotBumpKeyspaceVersion() public {
        startMaintenance(1, 0);
        completeMaintenance(1);
        assertKeyspaceVersion(0);
    }

    function test_completeMaintenanceDeletesMaintenance() public {
        startMaintenance(1, 0);
        completeMaintenance(1);
        assertNoMaintenance();
    }
    
    function test_completeMaintenanceEmitsMaintenanceCompletedEvent() public {
        startMaintenance(1, 0);
        vm.expectEmit();
        emit MaintenanceCompleted(vm.addr(1), 2);
        completeMaintenance(1);
    }

    // abortMaintenance

    function test_anyoneCanNotAbortMaintenance() public {
        startMaintenance(1, 0);
        expectRevert("not the owner");
        abortMaintenance(ANYONE);
    }

    function test_operatorCanNotAbortMaintenance() public {
        startMaintenance(1, 0);
        expectRevert("not the owner");
        abortMaintenance(OPERATOR);
    }

    function test_ownerCanAbortMaintenance() public {
        startMaintenance(1, 0);
        abortMaintenance(OWNER);
    }

    function test_canNotAbortNonExistentMaintenance() public {
        expectRevert("no maintenance");
        abortMaintenance(OWNER);
    }

    function test_abortMaintenanceBumpsVersion() public {
        startMaintenance(1, 0);
        abortMaintenance(OWNER);
        assertVersion(2);
    }

    function test_abortMaintenanceDoesNotBumpKeyspaceVersion() public {
        startMaintenance(1, 0);
        abortMaintenance(OWNER);
        assertKeyspaceVersion(0);
    }

    function test_abortMaintenanceDeletesMaintenance() public {
        startMaintenance(1, 0);
        abortMaintenance(OWNER);
        assertNoMaintenance();
    }

    function test_abortMaintenanceEmitsMaintenanceAbortedEvent() public {
        startMaintenance(1, 0);
        vm.expectEmit();
        emit MaintenanceAborted(2);
        abortMaintenance(OWNER);
    }

    // registerNodeOperator
    
    function test_anyoneCanRegisterNodeOperator() public {
        registerNodeOperator(ANYONE, "anyone");
    }

    function test_registerNodeOperatorDoesNotBumpVersion() public {
        registerNodeOperator(ANYONE, "anyone");
        assertVersion(0);
    }

    function test_registerNodeOperatorDoesNotBumpKeyspaceVersion() public {
        registerNodeOperator(ANYONE, "anyone");
        assertKeyspaceVersion(0);
    }

    function test_registerNodeOperatorDoesNotEmitEvents() public {
        vm.recordLogs();
        registerNodeOperator(ANYONE, "anyone");
        assertEq(vm.getRecordedLogs().length, 0);
    }

    // updateNodeOperatorData

    function test_anyoneCanNotUpdateNodeOperatorData() public {
        expectRevert("wrong operator");
        updateNodeOperatorData(ANYONE, 0, "new data");
    }

    function test_ownerCanNotUpdateNodeOperatorData() public {
        expectRevert("wrong operator");
        updateNodeOperatorData(OWNER, 0, "new data");
    }

    function test_operatorCanUpdateNodeOperatorData() public {
        updateNodeOperatorData(OPERATOR, 0, "new data");
    }

    function test_updateNodeOperatorDataDoesUpdateTheData() public {
        updateNodeOperatorData(OPERATOR, 0, "new data");
        assertKeyspaceSlot(clusterView.primaryKeyspace, 0, OPERATOR, "new data");
    }

    function test_updateNodeOperatorDataBumpsVersion() public {
        updateNodeOperatorData(OPERATOR, 0, "new data");
        assertVersion(1);
    }

    function test_updateNodeOperatorDataDoesNotBumpKeyspaceVersion() public {
        updateNodeOperatorData(OPERATOR, 0, "new data");
        assertKeyspaceVersion(0);
    }

    function test_updateNodeOperatorDataEmitsEventNodeOperatorDataUpdated() public {
        vm.expectEmit();
        emit NodeOperatorDataUpdated(vm.addr(OPERATOR), "new data", 1);
        updateNodeOperatorData(OPERATOR, 0, "new data");
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
        registerNodeOperator(10, "123456");
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
        startMigration(NEW_OWNER, newMigration().clear(0));
    }

    // full lifecycle

    function test_fullClusterLifecycle() public {
        updateNodeOperatorData(1, 0, "operator1");
        startMaintenance(2, 1);
        updateNodeOperatorData(3, 2, "operator3");
        completeMaintenance(2);

        registerNodeOperator(6, "operator6");
        registerNodeOperator(7, "operator7");
        registerNodeOperator(8, "operator8");
        startMigration(OWNER, newMigration().set(5, 6).set(6, 7).set(7, 8));
        updateNodeOperatorData(1, 0, "operator1'");
        for (uint256 i = 0; i < 8; i++) {
            completeMigration(i + 1, 1, uint8(i));
        }
        updateNodeOperatorData(3, 2, "operator3'");
        startMaintenance(7, 6);
        updateNodeOperatorData(7, 6, "operator8");
        abortMaintenance(OWNER);

        startMigration(OWNER, newMigration().clear(6));
        for (uint256 i = 0; i < 8; i++) {
            if (i != 6) {
                completeMigration(i + 1, 2, uint8(i));
            }
        }

        registerNodeOperator(9, "operator9");
        startMigration(OWNER, newMigration().set(6, 9));
        for (uint256 i = 0; i < 8; i++) {
            if (i != 6) {
                completeMigration(i + 1, 3, uint8(i));
            }
        }
        abortMigration(OWNER);

        transferOwnership(OWNER, NEW_OWNER);
        updateSettings(NEW_OWNER, Settings({ maxOperatorDataBytes: 1024 }));

        assertKeyspaceVersion(2);
    }

    // internal

    function newCluster(Vm vm) internal {   
        newCluster(vm, 5);
    }

    function newCluster(Vm vm, uint256 operatorsCount) internal {   
        newCluster(vm, Settings({ maxOperatorDataBytes: MAX_OPERATOR_DATA_BYTES }), operatorsCount);
    }

    function newCluster(Vm vm, Settings memory settings, uint256 operatorsCount) internal {
        setCaller(OWNER);

        address[] memory operators = new address[](operatorsCount);
        for (uint256 i = 0; i < operatorsCount; i++) {
            operators[i] = vm.addr(i + 1);
        }

        cluster = new Cluster(settings, operators);
        // ECRecover address. Constructor failed
        if (address(cluster) == address(1)) {
            return;
        }

        for (uint256 i = 0; i < operatorsCount; i++) {
            registerNodeOperator(i + 1);
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
        assertEq(clusterView.version, expectedVersion);
    }

    function assertKeyspaceVersion(uint64 expectedVersion) internal view {
        assertEq(clusterView.keyspaceVersion, expectedVersion);
    }

    function assertKeyspaceSlotsCount(KeyspaceView storage keyspace, uint256 count) internal view {
        assertEq(keyspace.operators.length, count);
    }

    function assertKeyspaceSlot(KeyspaceView storage keyspace, uint256 index, uint256 privateKey) internal view {
        assertKeyspaceSlot(keyspace, index, privateKey, DEFAULT_OPERATOR_DATA);
    }

    function assertKeyspaceSlot(KeyspaceView storage keyspace, uint256 index, uint256 privateKey, bytes memory data) internal view {
        assertEq(keyspace.operators[index].addr, vm.addr(privateKey));
        assertEq(keyspace.operators[index].data, data);
    }

    function assertKeyspaceSlotEmpty(KeyspaceView storage keyspace, uint256 index) internal view {
        assertEq(keyspace.operators[index].addr, address(0));
        assertEq(keyspace.operators[index].data, new bytes(0));
    }

    function assertMigration(uint64 id, uint256 pullingOperatorsCount) internal view {
        assertEq(clusterView.migration.id, id);
        assertEq(clusterView.migration.pullingOperatorsBitmask.count1(), pullingOperatorsCount);
    }

    function assertNoMigration() internal view {
        assertEq(clusterView.migration.id, 0);
        assertEq(clusterView.migration.pullingOperatorsBitmask, 0);
    }

    function assertMigrationPullingOperator(uint8 idx) internal view {
        assert(clusterView.migration.pullingOperatorsBitmask.is1(idx));
    }

    function assertMaintenance(uint256 operator) internal view {
        assertEq(clusterView.maintenance.slot, vm.addr(operator));
    }

    function assertNoMaintenance() internal view {
        assertEq(clusterView.maintenance.slot, address(0));
    }

    function newMigration() internal pure returns (TestMigration memory) {
        return TestMigration({
            vm: vm,
            plan: MigrationPlan({
                slotsToUpdate: new KeyspaceSlot[](0),
                replicationStrategy: 0
            })
        });
    } 

    function startMigration(uint256 caller, TestMigration memory migration) internal {
        setCaller(caller);
        cluster.startMigration(migration.plan);
        updateClusterView();
    }

    function completeMigration(uint256 caller, uint64 id, uint8 operatorIdx) internal {
        setCaller(caller);
        cluster.completeMigration(id, operatorIdx);
        updateClusterView();
    }

    function abortMigration(uint256 caller) internal {
        setCaller(caller);
        cluster.abortMigration();
        updateClusterView();
    }

    function startMaintenance(uint256 caller, uint8 operatorIdx) internal {
        setCaller(caller);
        cluster.startMaintenance(operatorIdx);
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

    function registerNodeOperator(uint256 caller) internal {
        registerNodeOperator(caller, DEFAULT_OPERATOR_DATA);
    }

    function registerNodeOperator(uint256 caller, bytes memory data) internal {
        setCaller(caller);
        cluster.registerNodeOperator(data);
        updateClusterView();
    }

    function updateNodeOperatorData(uint256 caller, uint8 idx, bytes memory data) internal {
        setCaller(caller);
        cluster.updateNodeOperatorData(idx, data);
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

    MigrationPlan plan;
}

library TestMigrationLib {
    function set(TestMigration memory self, uint8 idx, uint256 privateKey) internal pure returns (TestMigration memory) {
        TestMigrationLib.setSlot(self, idx, KeyspaceSlot({ idx: idx, operator: self.vm.addr(privateKey) }));
        return self;
    }

    function clear(TestMigration memory self, uint8 idx) internal pure returns (TestMigration memory) {
        TestMigrationLib.setSlot(self, idx, KeyspaceSlot({ idx: idx, operator: address(0) }));
        return self;
    }

    function setSlot(TestMigration memory self, uint8 idx, KeyspaceSlot memory slot) internal pure returns (TestMigration memory) {
        KeyspaceSlot[] memory slotsToUpdate = new KeyspaceSlot[](self.plan.slotsToUpdate.length + 1);
        for (uint256 i = 0; i < self.plan.slotsToUpdate.length; i++) {
            slotsToUpdate[i] = self.plan.slotsToUpdate[i];
        }

        slotsToUpdate[self.plan.slotsToUpdate.length] = slot;
        self.plan.slotsToUpdate = slotsToUpdate;

        return self;
    }
}

using TestMigrationLib for TestMigration;

function newNodeOperator(address addr, bytes memory operatorData) pure returns (NodeOperator memory) {
    return NodeOperator({
        addr: addr,
        data: operatorData
    });
}
