import pandas as pd
from typing import Any, Dict, List, Optional, Union

class KeywardApi:
    def __init__(self):
        self.connected = True
    
    async def create_table(self, table_name, columns):
        """Creates a table with specified columns"""
        from .table_operations import table_operations
        return await table_operations.create_table(table_name, columns)
    
    async def add_record(self, table_name, record):
        """Add a single record to a table"""
        from .table_operations import table_operations
        return await table_operations.add_data(table_name, record)
    
    async def add_records(self, table_name, records):
        """Add multiple records to a table"""
        from .table_operations import table_operations
        return await table_operations.add_data(table_name, records)
    
    async def get_table(self, table_name):
        """Get table data as a DataFrame"""
        from .table_operations import table_operations
        return await table_operations.fetch_table_to_dataframe(table_name)
    
    async def update_record(self, table_name, record_id, updates):
        """Update a record in a table"""
        from .table_operations import table_operations
        return await table_operations.update_table(table_name, record_id, updates)
    
    async def delete_record(self, table_name, record_id):
        """Delete a record from a table"""
        from .table_operations import table_operations
        return await table_operations.delete_data(table_name, record_id)
    
    async def merge_tables(self, sources, target=None, create_new=False):
        """Merge tables together"""
        from .table_operations import table_operations
        return await table_operations.merge_tables(sources, target, create_new)
    
    async def create_from_dataframe(self, table_name, dataframe):
        """Create a table from a pandas DataFrame"""
        from .table_operations import table_operations
        return await table_operations.create_table_from_dataframe(table_name, dataframe)
    
    async def bulk_update_records(self, table_name, updates):
        """Update multiple records at once"""
        from .table_operations import table_operations
        return await table_operations.bulk_update_records(table_name, updates)
    
    async def bulk_delete_records(self, table_name, record_ids):
        """Delete multiple records at once"""
        from .table_operations import table_operations
        return await table_operations.bulk_delete_records(table_name, record_ids)
    
    async def get_attachment_url(self, table_name, column_name, record_id):
        """Get URL for an attachment"""
        from .table_operations import table_operations
        return await table_operations.get_attachment_url(table_name, column_name, record_id)

# Create a singleton instance
api = KeywardApi()
