import pandas as pd

def nth_highest_salary(employee: pd.DataFrame, N: int) -> pd.DataFrame:
    distinct_salaries = (
        employee["salary"]
        .drop_duplicates()
        .sort_values(ascending=False)
        .reset_index(drop=True)
    )
    
    result = (
        distinct_salaries[N - 1]
        if N <= len(distinct_salaries) and N>0
        else pd.NA
    )
    
    return pd.DataFrame(
        {f"getNthHighestSalary({N})": [result]}
    )

# def nth_highest_salary(employee: pd.DataFrame, N: int) -> pd.DataFrame:
#     if N <=0 or employee['salary'].empty:
#         return pd.DataFrame({f'getNthHighestSalary({N})':[None]})
#     blah= employee['salary'].drop_duplicates().sort_values(ascending=False)
#     if len(blah) < N:
#         return pd.DataFrame({f'getNthHighestSalary({N})':[None]})
#     else:
#         return pd.DataFrame({f'getNthHighestSalary({N})':[blah.iloc[N-1]]})
