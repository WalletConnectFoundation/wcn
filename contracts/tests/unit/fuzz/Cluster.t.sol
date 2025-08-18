// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Test} from "forge-std/Test.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {ClusterHarness} from "tests/unit/ClusterHarness.sol";
import {Cluster, Settings, NodeOperator, Bitmask} from "src/Cluster.sol";
import {keyspace} from "tests/Utils.sol";

contract ClusterFuzzTest is Test {
    using Bitmask for uint256;

    ClusterHarness public cluster;
    
    function _defaultSettings() internal pure returns (Settings memory) {
        return Settings({
            maxOperatorDataBytes: 4096,
            minOperators: 1,
            extra: ""
        });
    }
    
    address constant OWNER = address(0x1);
    
    function setUp() public {
        cluster = _freshCluster(5);
    }
    
    /// @dev Helper that returns a cluster with N operators
    function _freshCluster(uint256 n) internal returns (ClusterHarness) {
        // Instantiate just to get the MAX_OPERATORS
        uint16 maxOperators = new ClusterHarness().MAX_OPERATORS();
        // Bound n to valid limits for most use cases
        // Allow MAX_OPERATORS for testing the limit
        if (n > maxOperators) {
            n = bound(n, 1, maxOperators);
        }
        
        NodeOperator[] memory operators = new NodeOperator[](n);
        for (uint256 i = 0; i < n; i++) {
            operators[i] = NodeOperator({
                addr: address(uint160(0x1000 + i)),
                data: abi.encodePacked("operator", i)
            });
        }

        ClusterHarness implementation = new ClusterHarness();
        bytes memory initData = abi.encodeCall(Cluster.initialize, (_defaultSettings(), operators));
        
        vm.prank(OWNER);
        ERC1967Proxy proxy = new ERC1967Proxy(address(implementation), initData);
        
        return ClusterHarness(address(proxy));
    }
    
    /*//////////////////////////////////////////////////////////////////////////
                            VALIDATE OPERATOR DATA FUZZ TESTS
    //////////////////////////////////////////////////////////////////////////*/
    
    /// @dev Fuzz test for _validateOperatorData with valid data sizes
    function testFuzz_ValidateOperatorData_ValidSizes(bytes calldata blob) public view {
        // Assume valid size range
        vm.assume(blob.length > 0 && blob.length <= _defaultSettings().maxOperatorDataBytes);
        
        // Should not revert for valid sizes
        cluster.exposed_validateOperatorData(blob);
    }
    
    /// @dev Fuzz test for _validateOperatorData with invalid empty data
    function testFuzz_ValidateOperatorData_EmptyData() public {
        bytes memory emptyData = "";
        
        vm.expectRevert(Cluster.InvalidOperatorData.selector);
        cluster.exposed_validateOperatorData(emptyData);
    }
    
    /// @dev Fuzz test for _validateOperatorData with oversized data
    function testFuzz_ValidateOperatorData_OversizedData(uint256 size) public {
        // Test sizes that exceed the limit
        size = bound(size, _defaultSettings().maxOperatorDataBytes + 1, _defaultSettings().maxOperatorDataBytes + 1000);
        
        bytes memory oversizedData = new bytes(size);
        // Fill with some data to make it non-trivial
        for (uint256 i = 0; i < size; i++) {
            oversizedData[i] = bytes1(uint8(i % 256));
        }
        
        vm.expectRevert(Cluster.InvalidOperatorData.selector);
        cluster.exposed_validateOperatorData(oversizedData);
    }
    
    /// @dev Fuzz test for _validateOperatorData against different settings
    function testFuzz_ValidateOperatorData_DifferentSettings(uint16 maxBytes, bytes calldata blob) public {
        // Create cluster with different settings
        maxBytes = uint16(bound(maxBytes, 1, 8192)); // Reasonable range
        Settings memory customSettings = Settings({
            maxOperatorDataBytes: maxBytes,
            minOperators: 1,
            extra: ""
        });
        
        ClusterHarness customCluster = _freshClusterWithSettings(3, customSettings);
        
        if (blob.length == 0) {
            vm.expectRevert(Cluster.InvalidOperatorData.selector);
            customCluster.exposed_validateOperatorData(blob);
        } else if (blob.length > maxBytes) {
            vm.expectRevert(Cluster.InvalidOperatorData.selector);
            customCluster.exposed_validateOperatorData(blob);
        } else {
            // Should not revert
            customCluster.exposed_validateOperatorData(blob);
        }
    }  
    
    /*//////////////////////////////////////////////////////////////////////////
                            KEYSPACE SORTING FUZZ TESTS
    //////////////////////////////////////////////////////////////////////////*/
    
    /// @dev Fuzz test that keyspace members are always sorted
    /// 
    /// WHAT THIS TEST VALIDATES:
    /// 1. Slot-based operator indexing works correctly with arbitrary slot combinations
    /// 2. Migration process maintains keyspace sorting invariant (critical for consensus)
    /// 3. Contract handles edge cases like slot 255 without overflow
    /// 4. Keyspace state transitions preserve data integrity
    function testFuzz_KeyspaceSorting_AlwaysSorted(uint8[] calldata operatorSlots) public {
        // STEP 1: Process fuzzer input into valid slot configuration
        // The fuzzer provides random uint8 values, we need to:
        // - Remove duplicates (same operator can't occupy multiple slots)
        // - Filter out slots beyond MAX_OPERATORS (256) 
        // - Sort them (required by startMigration validation)
        uint8[] memory validSlots = _filterAndSortSlots(operatorSlots);
        vm.assume(validSlots.length > 0);
        vm.assume(validSlots.length <= 10); // Keep test reasonable
        
        // STEP 2: Create cluster with sufficient operator capacity
        // We need to ensure every slot index in validSlots has a corresponding operator
        // If highest slot is 200, we need 201 operators (slots 0-200)
        uint8 highestSlotIndex = 0;
        for (uint256 i = 0; i < validSlots.length; i++) {
            if (validSlots[i] > highestSlotIndex) highestSlotIndex = validSlots[i];
        }
        
        // Need (highestSlotIndex + 1) operators to accommodate slot indices 0 through highestSlotIndex
        // Use uint256 to prevent overflow when highestSlotIndex is 255
        // CRITICAL: This prevents uint8(255 + 1) = 0 overflow
        uint256 requiredOperators = uint256(highestSlotIndex) + 1;
        
        ClusterHarness testCluster = _freshCluster(requiredOperators);
        
        // STEP 3: Avoid migration to identical keyspace (would revert with SameKeyspace)
        // Contract prevents no-op migrations to maintain state consistency
        (uint8[] memory currentMembers, uint8 currentStrategy) = testCluster.getCurrentKeyspace();
        
        // Skip if trying to migrate to the same keyspace
        bool isSameKeyspace = (currentMembers.length == validSlots.length) && (currentStrategy == 0);
        if (isSameKeyspace && currentMembers.length > 0) {
            for (uint256 i = 0; i < currentMembers.length; i++) {
                if (currentMembers[i] != validSlots[i]) {
                    isSameKeyspace = false;
                    break;
                }
            }
        }
        vm.assume(!isSameKeyspace);
        
        // STEP 4: Execute migration to test internal slot handling
        // This tests the critical state transition:
        // - Validates slots exist and are sorted (startMigration requirement)
        // - Updates keyspace storage with new slot configuration
        // - Increments keyspaceVersion for proper keyspace indexing
        // - Sets up migration tracking state
        vm.prank(OWNER);
        testCluster.startMigration(keyspace(validSlots, 0));
        
        // STEP 5: Verify internal sorting invariant is maintained
        (uint8[] memory keyspaceMembers,) = testCluster.getCurrentKeyspace();
        _assertArrayIsSorted(keyspaceMembers);
    }  
    
    /*//////////////////////////////////////////////////////////////////////////
                            OPERATOR COUNT INVARIANT FUZZ TESTS
    //////////////////////////////////////////////////////////////////////////*/
    
    /// @dev Fuzz test that operatorCount never exceeds MAX_OPERATORS  
    function testFuzz_OperatorCount_NeverExceedsMax() public {
        // Test exactly MAX_OPERATORS + 1 to ensure it fails
        uint256 attemptedOperators = uint256(cluster.MAX_OPERATORS()) + 1; // 257 operators
        
        NodeOperator[] memory operators = new NodeOperator[](attemptedOperators);
        
        // Verify array length is what we expect
        assertEq(operators.length, 257);
        
        // Fill array with valid operators
        for (uint256 i = 0; i < attemptedOperators; i++) {
            operators[i] = NodeOperator({
                addr: address(uint160(0x1000 + i)),
                data: abi.encodePacked("operator", i)
            });
        }

        ClusterHarness implementation = new ClusterHarness();
        bytes memory initData = abi.encodeCall(Cluster.initialize, (_defaultSettings(), operators));
        
        // Creating a cluster with more than MAX_OPERATORS should fail
        vm.expectRevert(Cluster.TooManyOperators.selector);
        
        vm.prank(OWNER);
        new ERC1967Proxy(address(implementation), initData);
    }
    
    /// @dev Fuzz test that MAX_OPERATORS is the actual limit
    function testFuzz_OperatorCount_MaxOperatorsIsLimit() public {
        // Test exactly MAX_OPERATORS to ensure it succeeds
        ClusterHarness maxCluster = _freshCluster(cluster.MAX_OPERATORS());
        
        assertEq(maxCluster.getOperatorCount(), cluster.MAX_OPERATORS());
        assertEq(maxCluster.getAllOperators().length, cluster.MAX_OPERATORS());
    }
    
    /// @dev Fuzz test that operator count matches getAllOperators length
    function testFuzz_OperatorCount_MatchesGetAllOperators(uint8 operatorCount) public {
        operatorCount = uint8(bound(operatorCount, 1, 50)); // Reasonable range for testing
        
        ClusterHarness testCluster = _freshCluster(operatorCount);
        
        assertEq(testCluster.getOperatorCount(), testCluster.getAllOperators().length);
        
        // Add an operator and verify invariant holds
        if (operatorCount < cluster.MAX_OPERATORS()) {
            NodeOperator memory newOp = NodeOperator({
                addr: address(uint160(0x9000 + operatorCount)),
                data: "new operator"
            });
            
            vm.prank(OWNER);
            testCluster.addNodeOperator(newOp);
            
            assertEq(testCluster.getOperatorCount(), testCluster.getAllOperators().length);
        }
        
        // Remove an operator (if possible) and verify invariant holds
        address[] memory operators = testCluster.getAllOperators();
        if (operators.length > _defaultSettings().minOperators) {
            // Find an operator not in the current keyspace
            for (uint256 i = 0; i < operators.length; i++) {
                try testCluster.removeNodeOperator(operators[i]) {
                    assertEq(testCluster.getOperatorCount(), testCluster.getAllOperators().length);
                    break;
                } catch {
                    // Operator might be in keyspace, try next one
                }
            }
        }
    }
    
    /*//////////////////////////////////////////////////////////////////////////
                            SLOT MANAGEMENT FUZZ TESTS
    //////////////////////////////////////////////////////////////////////////*/
    
    /// @dev Fuzz test for slot consistency
    function testFuzz_SlotConsistency_BidirectionalMapping(uint8 operatorCount) public {
        operatorCount = uint8(bound(operatorCount, 1, 20)); // Reasonable range
        
        ClusterHarness testCluster = _freshCluster(operatorCount);
        
        // Verify bidirectional mapping for all operators
        address[] memory operators = testCluster.getAllOperators();
        for (uint256 i = 0; i < operators.length; i++) {
            address operator = operators[i];
            uint8 slotIdx = testCluster.operatorSlotIndexes(operator);
            
            // Slot should be occupied
            assert(testCluster.operatorBitmask().isSet(slotIdx));
            
            // Slot should map back to the same operator
            (address addr, ) = testCluster.operatorSlots(slotIdx);
            assertEq(addr, operator);
        }
    }
    
    /// @dev Fuzz test for _isSlotInKeyspace correctness
    function testFuzz_IsSlotInKeyspace_Correctness(uint8 targetSlot, uint8 keyspaceIndex) public {
        targetSlot = uint8(bound(targetSlot, 0, 9)); // Use slots that exist
        keyspaceIndex = uint8(bound(keyspaceIndex, 0, 1)); // Only 2 keyspaces
        
        ClusterHarness testCluster = _freshCluster(10);
        
        bool result = testCluster.exposed_isSlotInKeyspace(targetSlot, keyspaceIndex);
        
        // Manually verify the result
        (uint8[] memory members,) = testCluster.getCurrentKeyspace();
        bool shouldBeInKeyspace = false;
        
        if (keyspaceIndex == testCluster.keyspaceVersion() % 2) {
            // Check current keyspace
            for (uint256 i = 0; i < members.length; i++) {
                if (members[i] == targetSlot) {
                    shouldBeInKeyspace = true;
                    break;
                }
            }
        }
        
        // Note: For non-current keyspaces, the result depends on the internal state
        // We mainly check that the function doesn't revert with valid inputs
        if (keyspaceIndex == testCluster.keyspaceVersion() % 2) {
            assertEq(result, shouldBeInKeyspace);
        }
    }
    
    /*//////////////////////////////////////////////////////////////////////////
                            HELPER FUNCTIONS
    //////////////////////////////////////////////////////////////////////////*/
    
    /// @dev Helper to create cluster with custom settings
    function _freshClusterWithSettings(uint256 n, Settings memory settings) internal returns (ClusterHarness) {
        n = bound(n, settings.minOperators, cluster.MAX_OPERATORS());
        
        NodeOperator[] memory operators = new NodeOperator[](n);
        for (uint256 i = 0; i < n; i++) {
            // Create valid operator data within the size limit
            bytes memory data = new bytes(bound(i + 1, 1, settings.maxOperatorDataBytes));
            for (uint256 j = 0; j < data.length; j++) {
                data[j] = bytes1(uint8((i + j) % 256));
            }
            
            operators[i] = NodeOperator({
                addr: address(uint160(0x2000 + i)),
                data: data
            });
        }

        ClusterHarness implementation = new ClusterHarness();
        bytes memory initData = abi.encodeCall(Cluster.initialize, (settings, operators));
        
        vm.prank(OWNER);
        ERC1967Proxy proxy = new ERC1967Proxy(address(implementation), initData);
        
        return ClusterHarness(address(proxy));
    }
    
    /// @dev Helper to filter and sort slots, removing duplicates and invalid entries
    function _filterAndSortSlots(uint8[] calldata slots) internal view returns (uint8[] memory) {
        if (slots.length == 0) return new uint8[](0);
        
        // Create array to track seen slots
        bool[256] memory seen;
        uint8[] memory temp = new uint8[](slots.length);
        uint256 count = 0;
        
        // Filter out duplicates and invalid slots
        for (uint256 i = 0; i < slots.length; i++) {
            if (slots[i] < cluster.MAX_OPERATORS() && !seen[slots[i]]) {
                seen[slots[i]] = true;
                temp[count] = slots[i];
                count++;
            }
        }
        
        // Create result array with correct size
        uint8[] memory result = new uint8[](count);
        for (uint256 i = 0; i < count; i++) {
            result[i] = temp[i];
        }
        
        // Sort the array
        for (uint256 i = 0; i < result.length; i++) {
            for (uint256 j = i + 1; j < result.length; j++) {
                if (result[i] > result[j]) {
                    uint8 temp_val = result[i];
                    result[i] = result[j];
                    result[j] = temp_val;
                }
            }
        }
        
        return result;
    }
    
    /// @dev Helper to assert an array is sorted
    function _assertArrayIsSorted(uint8[] memory arr) internal pure {
        for (uint256 i = 1; i < arr.length; i++) {
            require(arr[i] > arr[i-1], "Array is not sorted");
        }
    }
} 
