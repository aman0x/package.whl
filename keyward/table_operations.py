import pandas as pd
from typing import Any, Dict, List, Optional, Union

class TableOperations:
    @staticmethod
    async def create_table(table_name: str, columns: Dict[str, str]) -> bool:
        """
        Create a new table with specified columns.
        
        Args:
            table_name (str): Name of the table to create
            columns (dict): Dictionary of column names and types (e.g., {"Name": "Text"})
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            import grist.browser.api as gapi
            
            table_ops = gapi.TableOperations(gapi.grist, table_name)
            
            # Create a dummy record with the right structure
            dummy_record = {col_name: "" if col_type == "Text" else 0 for col_name, col_type in columns.items()}
            
            result = await table_ops.create([dummy_record])
            
            # Clean up the dummy record
            if result and len(result) > 0:
                await table_ops.destroy([result[0]])
                
            print(f"✅ Table '{table_name}' created with {len(columns)} columns")
            return True
        except Exception as e:
            print(f"❌ Error creating table: {e}")
            return False
    
    @staticmethod
    async def add_data(table_name: str, records: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Optional[List[int]]:
        """
        Add records to a table.
        
        Args:
            table_name (str): Table to insert into
            records (dict or list): Dict or list of dicts mapping column name to value
            
        Returns:
            List of new row IDs, or None on error
        """
        if not isinstance(records, list):
            records = [records]
            
        try:
            import grist.browser.api as gapi
            
            table_ops = gapi.TableOperations(gapi.grist, table_name)
            result = await table_ops.create(records)
            print(f"✅ Added {len(result)} records to '{table_name}'")
            return result
        except Exception as e:
            print(f"❌ Error adding data: {e}")
            return None
    
    @staticmethod
    async def fetch_table_to_dataframe(table_name: str) -> pd.DataFrame:
        """
        Fetch a table into a pandas DataFrame.
        
        Args:
            table_name (str): Table ID
            
        Returns:
            DataFrame without system columns
        """
        try:
            import grist.browser.api as gapi
            
            table_data = await gapi.grist.fetch_table(table_name)
            df = pd.DataFrame({k: v for k, v in table_data.items() 
                              if k not in ['id', 'manualSort']})
            return df
        except Exception as e:
            print(f"❌ Error fetching table: {e}")
            return pd.DataFrame()
    
    @staticmethod
    async def update_table(table_name: str, row_id: int, updates: Dict[str, Any]) -> bool:
        """
        Update a single row's fields.
        
        Args:
            table_name (str): Table containing the row
            row_id (int): Numeric ID of the row
            updates (dict): Dict of column->new value
            
        Returns:
            True on success, False on error
        """
        try:
            import grist.browser.api as gapi
            
            table_ops = gapi.TableOperations(gapi.grist, table_name)
            await table_ops.update([{"id": row_id, **updates}])
            return True
        except Exception as e:
            print(f"❌ Error updating record: {e}")
            return False
    
    @staticmethod
    async def delete_data(table_name: str, row_ids: Union[int, List[int]]) -> bool:
        """
        Delete rows by explicit IDs.
        
        Args:
            table_name (str): Table ID
            row_ids (int or list): Row ID or list of row IDs to remove
            
        Returns:
            True on success, False on error
        """
        try:
            import grist.browser.api as gapi
            
            table_ops = gapi.TableOperations(gapi.grist, table_name)
            await table_ops.destroy(row_ids if isinstance(row_ids, list) else [row_ids])
            return True
        except Exception as e:
            print(f"❌ Error deleting data: {e}")
            return False

    @staticmethod
    async def create_table_from_dataframe(table_name: str, df: pd.DataFrame) -> Optional[str]:
        """
        Create a new table from a DataFrame and populate it.
        
        Args:
            table_name (str): Desired table ID
            df (DataFrame): Source DataFrame
            
        Returns:
            Table ID on success, None on error
        """
        if df.empty:
            return None
            
        try:
            # Map pandas dtypes to our types
            type_map = {
                'int64': 'Numeric',
                'int32': 'Numeric',
                'float64': 'Numeric',
                'float32': 'Numeric',
                'bool': 'Bool',
                'datetime64[ns]': 'Date',
                'object': 'Text',
                'category': 'Text',
            }
            
            # Create the table
            cols = {col: type_map.get(str(dt), 'Text') for col, dt in df.dtypes.items()}
            if not await TableOperations.create_table(table_name, cols):
                return None
                
            # Add the data
            records = df.replace({pd.NA: None}).to_dict('records')
            if await TableOperations.add_data(table_name, records) is None:
                return None
                
            return table_name
        except Exception as e:
            print(f"❌ Error creating table from DataFrame: {e}")
            return None
            
    @staticmethod
    async def bulk_add_records(table_name: str, records: List[Dict[str, Any]]) -> Optional[List[int]]:
        """
        Add multiple records to a table in a single operation.
        
        Args:
            table_name (str): Table to insert into
            records (list): List of dicts mapping column name to value
            
        Returns:
            List of new row IDs, or None on error
        """
        return await TableOperations.add_data(table_name, records)
            
    @staticmethod
    async def bulk_update_records(table_name: str, updates: List[Dict[str, Any]]) -> List[bool]:
        """
        Apply multiple row updates in a single operation.
        
        Args:
            table_name (str): The table to update
            updates (list): List of dicts each with 'id' key for row ID plus fields to update
            
        Returns:
            List of booleans indicating success of each update
        """
        results = []
        try:
            import grist.browser.api as gapi
            
            table_ops = gapi.TableOperations(gapi.grist, table_name)
            await table_ops.update(updates)
            return [True] * len(updates)
        except Exception as e:
            print(f"❌ Error in bulk update: {e}")
            return [False] * len(updates)
            
    @staticmethod
    async def bulk_delete_records(table_name: str, row_ids: List[int]) -> bool:
        """
        Delete multiple rows in a single operation.
        
        Args:
            table_name (str): Table ID
            row_ids (list): List of row IDs to remove
            
        Returns:
            True on success, False on error
        """
        return await TableOperations.delete_data(table_name, row_ids)
            
    @staticmethod
    async def bulk_delete_where(table_name: str, where: Dict[str, Any]) -> bool:
        """
        Delete rows matching specific criteria.
        
        Args:
            table_name (str): Table ID
            where (dict): Dict of column->value to match for deletion
            
        Returns:
            True on success, False on error
        """
        try:
            import grist.browser.api as gapi
            
            # Fetch the table to find matching rows
            table_data = await gapi.grist.fetch_table(table_name)
            
            # Find rows that match all criteria
            row_ids = []
            for i, row_id in enumerate(table_data.get('id', [])):
                match = True
                for col, val in where.items():
                    if col in table_data and i < len(table_data[col]):
                        if table_data[col][i] != val:
                            match = False
                            break
                    else:
                        match = False
                        break
                
                if match:
                    row_ids.append(row_id)
                    
            # Delete the matching rows
            if row_ids:
                return await TableOperations.delete_data(table_name, row_ids)
            return True
        except Exception as e:
            print(f"❌ Error in bulk delete where: {e}")
            return False
            
    @staticmethod
    async def merge_tables(sources: List[str], target: Optional[str] = None, create_new: bool = False) -> bool:
        """
        Append rows from multiple source tables into a single target table.
        
        Args:
            sources (list): List of source table IDs
            target (str, optional): Target table ID (first source if None)
            create_new (bool): Whether to create a new target table
            
        Returns:
            True on success, False on error
        """
        try:
            import grist.browser.api as gapi
            
            if len(sources) < 1:
                return False
                
            tgt = target if target else sources[0]
            srcs = sources if target else sources[1:]
            
            # Create target table if needed
            if create_new and srcs:
                # Fetch schema from first source
                first_source_data = await gapi.grist.fetch_table(srcs[0])
                
                # Determine column types
                type_map = {
                    'int': 'Numeric',
                    'float': 'Numeric',
                    'bool': 'Bool',
                    'str': 'Text',
                    'NoneType': 'Text'
                }
                
                cols = {}
                for col in first_source_data:
                    if col not in ('id', 'manualSort'):
                        # Get non-None value to determine type
                        sample = next((v for v in first_source_data[col] if v is not None), None)
                        col_type = type_map.get(type(sample).__name__, 'Text')
                        cols[col] = col_type
                
                # Create the target table
                await TableOperations.create_table(tgt, cols)
            
            # Copy data from each source
            for src in srcs:
                src_data = await gapi.grist.fetch_table(src)
                
                # Convert to records
                records = []
                row_count = len(src_data.get('id', []))
                
                for i in range(row_count):
                    record = {}
                    for col in src_data:
                        if col not in ('id', 'manualSort') and i < len(src_data[col]):
                            record[col] = src_data[col][i]
                    records.append(record)
                
                # Add to target table
                if records:
                    await TableOperations.add_data(tgt, records)
            
            return True
        except Exception as e:
            print(f"❌ Error merging tables: {e}")
            return False
    
    @staticmethod
    async def merge_tables_strict(sources: List[str], target: Optional[str] = None, create_new: bool = False) -> bool:
        """
        Merge tables using only columns that exist in all tables.
        
        Args:
            sources (list): List of source table IDs
            target (str, optional): Target table ID (first source if None)
            create_new (bool): Whether to create a new target table
            
        Returns:
            True on success, False on error
        """
        try:
            import grist.browser.api as gapi
            
            if len(sources) < 1:
                return False
                
            tgt = target if target else sources[0]
            srcs = sources if target else sources[1:]
            
            # Find common columns across all sources
            common_cols = None
            
            for src in sources:
                src_data = await gapi.grist.fetch_table(src)
                src_cols = set(col for col in src_data if col not in ('id', 'manualSort'))
                
                if common_cols is None:
                    common_cols = src_cols
                else:
                    common_cols &= src_cols
            
            if not common_cols:
                print("❌ No common columns found")
                return False
                
            # Create target table if needed
            if create_new:
                # Fetch schema from first source
                first_source_data = await gapi.grist.fetch_table(sources[0])
                
                # Determine column types for common columns
                type_map = {
                    'int': 'Numeric',
                    'float': 'Numeric',
                    'bool': 'Bool',
                    'str': 'Text',
                    'NoneType': 'Text'
                }
                
                cols = {}
                for col in common_cols:
                    # Get non-None value to determine type
                    sample = next((v for v in first_source_data[col] if v is not None), None)
                    col_type = type_map.get(type(sample).__name__, 'Text')
                    cols[col] = col_type
                
                # Create the target table
                await TableOperations.create_table(tgt, cols)
            
            # Copy data from each source
            for src in srcs:
                src_data = await gapi.grist.fetch_table(src)
                
                # Convert to records with only common columns
                records = []
                row_count = len(src_data.get('id', []))
                
                for i in range(row_count):
                    record = {}
                    for col in common_cols:
                        if i < len(src_data[col]):
                            record[col] = src_data[col][i]
                    records.append(record)
                
                # Add to target table
                if records:
                    await TableOperations.add_data(tgt, records)
            
            return True
        except Exception as e:
            print(f"❌ Error in strict merge: {e}")
            return False
    
    @staticmethod
    async def get_attachment_url(table_name: str, column_name: str, row_id: int) -> Optional[str]:
        """
        Get the URL for an attachment.
        
        Args:
            table_name (str): Table ID
            column_name (str): Column name containing the attachment
            row_id (int): Row ID
            
        Returns:
            URL string or None on error
        """
        try:
            import grist.browser.api as gapi
            
            # Fetch the specific record
            record = await gapi.grist.fetch_selected_record(row_id)
            
            if record and column_name in record:
                attachment = record[column_name]
                if attachment:
                    # Return the URL if available
                    return attachment.get('url')
            
            return None
        except Exception as e:
            print(f"❌ Error getting attachment URL: {e}")
            return None
            
    @staticmethod
    async def bulk_merge_tables(tables: List[Dict[str, Any]]) -> bool:
        """
        Merge multiple tables with customized settings.
        
        Args:
            tables (list): List of dicts with source, target, and options
            
        Returns:
            True on success, False on error
        """
        for config in tables:
            sources = config.get('sources', [])
            target = config.get('target')
            create_new = config.get('create_new', False)
            strict = config.get('strict', False)
            
            if strict:
                success = await TableOperations.merge_tables_strict(sources, target, create_new)
            else:
                success = await TableOperations.merge_tables(sources, target, create_new)
                
            if not success:
                return False
                
        return True

# Create an instance
table_operations = TableOperations()
