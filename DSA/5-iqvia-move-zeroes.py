# Way 1: Using a list of non zero 
li=[1, 0, 2, 0, 0, 7]
nonzero_indies=[]
for i in range(len(li)):
    if li[i] != 0:
        nonzero_indies.append(i)

for j in range(0, len(li)):
    if j < len(nonzero_indies):
        li[j] = li[nonzero_indies[j]]
    else:
        li[j] = 0

print(li)


# Way 3: Using two pointers to mark last zero and last non-zero
# This is a more efficient way to move zeroes to the end of the list
def move_zeroes(nums):
    print(nums)
    nzi=-1
    zi=-1
    last_nzi=-1
    last_zi=zi
    for i in range(0,len(nums)):
        print(i, nums[i])
        if nums[i]==0:
            zi = i
            if last_zi == -1:
                last_zi = i
        else:
            nzi = i
            if last_nzi == -1:
                last_nzi = i
        print("nzi: ",nzi,"zi: ",zi,"last_nzi: ", last_nzi, "last_zi: ", last_zi)
        if nums[i] != 0:
            if zi != -1:
                nums[last_zi] = nums[i]
                nums[i] = 0

                last_nzi = last_zi
                last_zi = last_nzi + 1
                print("After Swap: nzi: ",nzi,"zi: ",zi,"last_nzi: ", last_nzi, "last_zi: ", last_zi)
        print(nums)

move_zeroes([0, 1, 0, 2, 0, 0, 7])
print("------------------------------------------------------------")
move_zeroes([10,0,2,0,0,7])













   