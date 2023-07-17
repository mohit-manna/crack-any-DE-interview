# https://leetcode.com/problems/two-sum/

# Given an array of integers nums and an integer target, return indices of the two numbers such that they add up to target.
# Input: nums = [2,7,11,15], target = 9
# Output: [0,1]
# Explanation: Because nums[0] + nums[1] == 9, we return [0, 1].


# nums = [2,7,11,15]
# target = 9
nums=[2,4,3]
target=7
def twoSum(nums,target):
    seen = {}
    for i in range(len(nums)):
        diff = target-nums[i]  #9-2,9-7,9-11,9-15
        if seen.get(diff,-1)>=0:
            return [seen[diff],i]
        else:
            seen[nums[i]]=i

print(twoSum(nums,target))