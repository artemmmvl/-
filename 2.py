def insertion_sort(intervals):

    for i in range(1, len(intervals)):
        key = intervals[i]
        j = i - 1
        while j >= 0 and key[0] < intervals[j][0]:
            intervals[j + 1] = intervals[j]
            j -= 1
        intervals[j + 1] = key
    return intervals


def merge_intervals(input_list):
    if not input_list:
        return []

    sorted_list = insertion_sort(input_list)

    output_list = [sorted_list[0]]

    for current in sorted_list[1:]:
        last = output_list[-1]

        if current[0] <= last[1]:
            last[1] = max(last[1], current[1])
        else:
            output_list.append(current)

    return output_list


input_list = [[1, 3], [2, 6], [9, 11], [8, 9], [2, 5], [10, 15], [16, 19]]
print(merge_intervals(input_list))
