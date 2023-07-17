# https://leetcode.com/problems/3sum/

# Given an integer array nums, return all the triplets [nums[i], nums[j], nums[k]] such that i != j, i != k, and j != k, and nums[i] + nums[j] + nums[k] == 0.
# Notice that the solution set must not contain duplicate triplets.
# nums = [-1,0,1,2,-1,-4]
# Output: [[-1,-1,2],[-1,0,1]]

# nums = [0,1,1]
# Output: []

# Logic: Sort the input and use every negative number as key of dict
from typing import List
# nums = [-1,0,1,2,-1,-4]
nums= [-2,-2,0,0,2,2]


def threeSum(nums: List[int]) -> List[List[int]]:
    nums.sort()
    res = []
    for i,a in enumerate(nums):
        if i>0 and a==nums[i-1]:
            continue
        l,r=i+1, len(nums)-1
        while l< r:
            threeSum = a + nums[l] + nums [r]
            if threeSum > 0 :
                r -= 1
            elif threeSum < 0 :
                l += 1 
            else : 
                res.append([a, nums[l],nums[r]]) # one solution added
                # to look for other combination we append the left ptr
                # print(l,r,res)
                l += 1
                while nums[l] == nums[l - 1] and l < r:
                    # print("l: ",l)
                    l+=1
    return res

    
    return [0]



print(threeSum(nums))