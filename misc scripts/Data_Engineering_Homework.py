#import libraries
import pandas as pd

## check for duplicate function definition

def check_duplicates(df, columns):
    # Check inputs
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input df must be a pandas DataFrame.")
    if not isinstance(columns, list):
        raise TypeError("Input columns must be a list.")
    if not all(isinstance(col, str) for col in columns):
        raise TypeError("All elements in columns must be string.")
    if not all(col in df.columns for col in columns):
        raise ValueError("All elements in columns must correspond to column names in the DataFrame.")

    try:
        # Check for duplicates
        duplicates = df.duplicated(subset=columns, keep='first')

        # Count the number of duplicate rows
        count = duplicates.sum()

        # Create a dataframe with group count of duplicate rows
        samples = df[duplicates].groupby(columns).size().reset_index(name='number_of_duplicates')

        return {'count': count, 'samples': samples}
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


if __name__ == "__main__":

    df = pd.DataFrame(
        data=[
            ['A','a', 'x', 1],
            ['A','b', 'x', 1],
            ['A','c', 'x', 1],
            ['B','a', 'x', 1],
            ['B','b', 'x', 1],
            ['B','c', 'x', 1],
            ['A','a', 'y', 1],
        ],
        columns=['col_1', 'col_2', 'col_3', 'col_4']
    )
    
    # Use case 1
    result = check_duplicates(df, ['col_1'])
    print("Duplicates check for ['col_1']: ")
    print("Count: ", result["count"])
    print("Samples: ")
    print(result["samples"])

    # Use case 2
    result = check_duplicates(df, ['col_1', 'col_2'])
    print("\nDuplicates check for ['col_1', 'col_2']: ")
    print("Count: ", result["count"])
    print("Samples: ")
    print(result["samples"])

    # Use case 3
    result = check_duplicates(df, ['col_1', 'col_2', 'col_3'])
    print("\nDuplicates check for ['col_1', 'col_2', 'col_3']: ")
    print("Count: ", result["count"])
    print("Samples: ")
    print(result["samples"])