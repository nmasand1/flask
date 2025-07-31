

# import pandas as pd
# import yaml
# import os

# import pandas as pd
# import os

# class CSVComparator:
#     def __init__(self, csv1_path, csv2_path, columns, data_start_row_csv1=0, data_start_row_csv2=0, order=1):
#         self.files = {'CSV1': csv1_path, 'CSV2': csv2_path}
#         self.columns = [col.strip().lower().replace(" ", "") for col in columns.split(',')]
#         self.data_start_row = [data_start_row_csv1, data_start_row_csv2]
#         self.output_dir = 'output_files'
#         self.order = order
#         os.makedirs(self.output_dir, exist_ok=True)



#     def load_config(self):
#         with open(self.config_file, 'r') as file:
#             config = yaml.safe_load(file)
#             self.files = {list(file.keys())[0]: file[list(file.keys())[0]] for file in config['files']}
#             self.columns = [col.strip().lower().replace(" ", "") for col in config['columns']]
#             self.data_start_row = config['data_start_row']
#             if len(self.data_start_row) != len(self.files):
#                 raise ValueError("data_start_row should have the same number of elements as files.")

#     def normalize_column_names(self, df):
#         df.columns = df.columns.str.lower().str.replace(" ", "").str.strip()
#         return df

#     def normalize_data(self, df):
#         for col in df.columns:
#             df[col] = df[col].astype(str).str.lower().str.strip().str.replace(",", "")
#         return df

#     def load_csv_files(self):
#         dataframes = {}
#         for key, file_name in self.files.items():
#             try:
#                 df = pd.read_csv(file_name, header=self.data_start_row[list(self.files.keys()).index(key)], low_memory=False)
#                 df = self.normalize_column_names(df)
#                 dataframes[key] = df
#             except FileNotFoundError as e:
#                 print(f"Error loading CSV file: {e}")
#             except Exception as e:
#                 print(f"An unexpected error occurred while loading the CSV Files: {e}")
#         return dataframes

#     def reorder_files(self, dataframes):
#         keys = list(dataframes.keys())
#         if self.order == 2:
#             return {keys[1]: dataframes[keys[1]], keys[0]: dataframes[keys[0]]}
#         return dataframes

#     def check_missing_columns(self, dataframes):
#         missing_columns = {}
#         for key, df in dataframes.items():
#             missing_columns[key] = [col for col in self.columns if col not in df.columns]
#         for key, missing in missing_columns.items():
#             if missing:
#                 print(f"Missing columns in the file '{key}': {missing}")
#         return missing_columns

#     def compare_columns(self, df1, df2):
#         df1[self.columns] = df1[self.columns].fillna('').astype(str).apply(lambda x: x.str.strip().str.lower())
#         df2[self.columns] = df2[self.columns].fillna('').astype(str).apply(lambda x: x.str.strip().str.lower())

#         df1_deduped = df1.drop_duplicates(subset=self.columns)
#         df2_deduped = df2.drop_duplicates(subset=self.columns)

#         # Perform the merge operation
#         comparison_df = pd.merge(df1_deduped, df2_deduped, on=self.columns, how='outer', indicator=True)
#         only_in_df1 = comparison_df[comparison_df['_merge'] == 'left_only']
#         only_in_df2 = comparison_df[comparison_df['_merge'] == 'right_only']
#         in_both = comparison_df[comparison_df['_merge'] == 'both']
#         return only_in_df1, only_in_df2, in_both

#     def create_result_df(self, only_in_df1, only_in_df2, key1, key2):
#         max_len = max(len(only_in_df1), len(only_in_df2))
#         result_df = pd.DataFrame(index=range(max_len))

#         if self.order == 1:
#             left_prefix = f"({key1}) "
#             right_prefix = f"({key2})_"
#             left_data = only_in_df1
#             right_data = only_in_df2
#         else:
#             left_prefix = f"({key2}) "
#             right_prefix = f"({key1})_"
#             left_data = only_in_df2
#             right_data = only_in_df1

#         for col in self.columns:
#             left_col_values = left_data[col].reset_index(drop=True) if col in left_data.columns else pd.Series([None] * max_len)
#             right_col_values = right_data[col].reset_index(drop=True) if col in right_data.columns else pd.Series([None] * max_len)
#             result_df[f'{left_prefix}{col}'] = left_col_values
#             result_df[f'{right_prefix}{col}'] = right_col_values

#         result_df['Result'] = ['Not matching'] * max_len
#         result_df['Final Result'] = 'Columns not identical'
#         return result_df

#     def save_and_print_csv(self, df, filename):
#         output_path = os.path.join(self.output_dir, filename)
#         try:
#             df.to_csv(output_path, index=False)
#             print(f"{output_path} saved with {df.shape[0]} rows and {df.shape[1]} columns.")
#         except IOError as e:
#             print(f"Error saving file: {e}")

#     def calculate_stats(self, in_both, only_in_df1, only_in_df2):
#         total_rows = len(in_both) + len(only_in_df1) + len(only_in_df2)
#         matching_percentage = (len(in_both) / total_rows) * 100 if total_rows > 0 else 0
#         non_matching_percentage = 100 - matching_percentage
#         stats_df = pd.DataFrame({
#             'Total Rows': [total_rows],
#             'Matching Rows': [len(in_both)],
#             'Non-Matching Rows': [len(only_in_df1) + len(only_in_df2)],
#             'Matching Percentage': [matching_percentage],
#             'Non-Matching Percentage': [non_matching_percentage]
#         })
#         return stats_df

#     def run_comparison(self):
#         try:
#             dataframes = self.load_csv_files()
#             dataframes = self.reorder_files(dataframes)

#             if any(df.empty for df in dataframes.values()):
#                 raise ValueError("One or more DataFrames are empty or could not be loaded properly.")

#             if self.check_missing_columns(dataframes):
#                 keys = list(dataframes.keys())
#                 only_in_df1, only_in_df2, in_both = self.compare_columns(dataframes[keys[0]], dataframes[keys[1]])

#                 if only_in_df1 is not None and only_in_df2 is not None:
#                     result_df = self.create_result_df(only_in_df1, only_in_df2, keys[0], keys[1])
#                     self.save_and_print_csv(result_df, "non_matching_rows.csv")
#                     match_both = in_both[self.columns]
#                     self.save_and_print_csv(match_both, "matching_rows.csv")
#                     stats_df = self.calculate_stats(in_both, only_in_df1, only_in_df2)
#                     self.save_and_print_csv(stats_df, "comparison_stats.csv")
#                 else:
#                     print("One or more specified columns are missing in one or both CSV files.")
#         except Exception as e:
#             print(f"An error occurred during the comparison process: {e}")





import pandas as pd
import os
import yaml


class CSVComparator:
    def __init__(self, csv1_path, csv2_path, columns, data_start_row_csv1=0, data_start_row_csv2=0, order=1):
        self.files = {'CSV1': csv1_path, 'CSV2': csv2_path}
        self.columns = [col.strip().lower().replace(" ", "") for col in columns.split(',')]
        self.data_start_row = [data_start_row_csv1, data_start_row_csv2]
        self.output_dir = 'output_files'
        self.order = order
        os.makedirs(self.output_dir, exist_ok=True)

    @staticmethod
    def normalize_column_names(df):
        df.columns = df.columns.str.lower().str.replace(" ", "").str.strip()
        return df

    @staticmethod
    def normalize_data(df):
        for col in df.columns:
            df[col] = df[col].astype(str).str.lower().str.strip().str.replace(",", "")
        return df

    def detect_delimiter(self, file_path):
        """Detect the delimiter used in the CSV file"""
        common_delimiters = [',', '^', ';', '\t', '|']
        with open(file_path, 'r', encoding='utf-8') as file:
            first_line = file.readline()
            for delimiter in common_delimiters:
                if delimiter in first_line:
                    return delimiter
        return ','  # Default to comma if no delimiter is detected
    
    def remove_duplicates(self, df, key):
        """Remove duplicate rows from dataframe and print statistics"""
        original_count = len(df)
        df_deduplicated = df.drop_duplicates()
        duplicates_removed = original_count - len(df_deduplicated)
        
        if duplicates_removed > 0:
            print(f"Removed {duplicates_removed} duplicate rows from {key}")
            print(f"Original rows: {original_count}, After deduplication: {len(df_deduplicated)}")
        else:
            print(f"No duplicates found in {key}")
            
        return df_deduplicated
    
    def load_csv_files(self):
        dataframes = {}
        for key, file_name in self.files.items():
            try:
                # Detect delimiter for this file
                delimiter = self.detect_delimiter(file_name)
                print(f"Detected delimiter '{delimiter}' for {key}: {file_name}")
                
                # Load CSV with detected delimiter
                df = pd.read_csv(file_name, 
                               header=self.data_start_row[list(self.files.keys()).index(key)], 
                               delimiter=delimiter,
                               low_memory=False)
                
                # Normalize column names
                df = self.normalize_column_names(df)
                
                # Remove duplicates before storing
                df = self.remove_duplicates(df, key)
                
                dataframes[key] = df
            except FileNotFoundError as e:
                print(f"Error loading CSV file: {e}")
            except Exception as e:
                print(f"An unexpected error occurred while loading the CSV Files: {e}")
        return dataframes

    def reorder_files(self, dataframes):
        keys = list(dataframes.keys())
        if self.order == 2:
            return {keys[1]: dataframes[keys[1]], keys[0]: dataframes[keys[0]]}
        return dataframes

    def check_missing_columns(self, dataframes):
        missing_columns = {}
        for key, df in dataframes.items():
            missing_columns[key] = [col for col in self.columns if col not in df.columns]
        for key, missing in missing_columns.items():
            if missing:
                print(f"Missing columns in the file '{key}': {missing}")
        return missing_columns

    def compare_columns_independently(self, df1, df2):
        results = {}
        for col in self.columns:
            if col in df1.columns and col in df2.columns:
                df1_col = df1[[col]].drop_duplicates().rename(columns={col: f'({list(self.files.keys())[0]}) {col}'})
                df2_col = df2[[col]].drop_duplicates().rename(columns={col: f'({list(self.files.keys())[1]}) {col}'})
                comparison = pd.merge(df1_col, df2_col, left_on=f'({list(self.files.keys())[0]}) {col}',
                                      right_on=f'({list(self.files.keys())[1]}) {col}', how='outer', indicator=True)
                results[col] = {
                    'only_in_df1': comparison[comparison['_merge'] == 'left_only'],
                    'only_in_df2': comparison[comparison['_merge'] == 'right_only'],
                    'in_both': comparison[comparison['_merge'] == 'both']
                }
        return results

    def save_and_print_csv(self, df, filename):
        output_path = os.path.join(self.output_dir, filename)
        try:
            df.to_csv(output_path, index=False)
            print(f"{output_path} saved with {df.shape[0]} rows and {df.shape[1]} columns.")
        except IOError as e:
            print(f"Error saving file: {e}")

    def calculate_stats_independently(self, comparison_results):
        stats = []
        for col, result in comparison_results.items():
            total_rows = len(result['in_both']) + len(result['only_in_df1']) + len(result['only_in_df2'])
            matching_percentage = (len(result['in_both']) / total_rows) * 100 if total_rows > 0 else 0
            non_matching_percentage = 100 - matching_percentage
            stats.append({
                'Column': col,
                'Total Rows': total_rows,
                'Matching Rows': len(result['in_both']),
                'Non-Matching Rows': len(result['only_in_df1']) + len(result['only_in_df2']),
                'Matching Percentage': matching_percentage,
                'Non-Matching Percentage': non_matching_percentage
            })
        return pd.DataFrame(stats)

    def run_comparison(self):
        try:
            dataframes = self.load_csv_files()
            dataframes = self.reorder_files(dataframes)

            if any(df.empty for df in dataframes.values()):
                raise ValueError("One or more DataFrames are empty or could not be loaded properly.")

            if self.check_missing_columns(dataframes):
                comparison_results = self.compare_columns_independently(dataframes['CSV1'], dataframes['CSV2'])
                non_matching_rows = pd.DataFrame()
                for col, result in comparison_results.items():
                    if not result['only_in_df1'].empty or not result['only_in_df2'].empty:
                        merged = pd.concat([result['only_in_df1'], result['only_in_df2']], ignore_index=True)
                        non_matching_rows = pd.concat([non_matching_rows, merged], ignore_index=True)

                if not non_matching_rows.empty:
                    self.save_and_print_csv(non_matching_rows, "non_matching_rows.csv")

                matching_rows = pd.concat([result['in_both'] for result in comparison_results.values() if not result['in_both'].empty], ignore_index=True)
                if not matching_rows.empty:
                    self.save_and_print_csv(matching_rows, "matching_rows.csv")

                stats_df = self.calculate_stats_independently(comparison_results)
                self.save_and_print_csv(stats_df, "comparison_stats.csv")
        except Exception as e:
            print(f"An error occurred during the comparison process: {e}")
