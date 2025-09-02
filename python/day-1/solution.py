class Solution:
    def twoSum(self, nums: List[int], target: int) -> List[int]:
        num_map = {} # created a dictionary to check with hashmap logic instead of checking the whole numbers
        for i, v in enumerate(nums):
            complement = target - v # checking the complement of target and current value
            if complement in num_map:
                return (num_map[complement],i) #if matches return the existing index value of the matched number and the current loop value
            num_map[v] = i  # if not present add it as actual number as key and index as value
