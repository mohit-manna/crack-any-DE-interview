# https://leetcode.com/problems/two-sum-ii-input-array-is-sorted/

# Given a sorted array of integers nums and an integer target, return indices of the two numbers such that they add up to target.
# Input: nums = [2,7,11,15], target = 9
# Output: [1,2]
# Explanation: Because nums[0] + nums[1] == 9, we return [1, 2].


nums = [2,7,11,15]
target = 9
# nums=[2,4,3]
# target=7
def twoSum(nums,target):
    seen = {}
    for i in range(len(nums)):
        diff = target-nums[i]  #9-2,9-7,9-11,9-15
        if seen.get(diff,-1)>=0:
            return [seen[diff],i+1]
        else:
            seen[nums[i]]=i+1

print(twoSum(nums,target))