
import os
import logging
from typing import List, Dict, Optional, Union
from supabase import create_client, Client
from supabase.client import ClientOptions
from dotenv import load_dotenv

load_dotenv()

class SupabaseManager:
    def __init__(self, url: str = None, key: str = None, default_schema: Optional[str] = None):
        """
        Khởi tạo Supabase Manager
        Args:
            url: Supabase URL
            key: Supabase API Key (khuyến nghị sb_secret_... cho pipeline)
            default_schema: nếu cung cấp, sẽ khởi tạo client mặc định với schema này
        """
        self.url = url or os.getenv("SUPABASE_URL")
        self.key = key or os.getenv("SUPABASE_KEY")
        
        self.default_schema = default_schema
        # cache client instances per schema for flexible multi-schema usage
        self._clients: Dict[Optional[str], Client] = {}
        
        if not self.url or not self.key:
            raise ValueError("Supabase URL và Key là bắt buộc")
            
        # create initial client (respect default_schema if provided)
        if self.default_schema:
            initial_client = create_client(self.url, self.key, options=ClientOptions(schema=self.default_schema))
        else:
            initial_client = create_client(self.url, self.key)

        self._clients[self.default_schema] = initial_client
        # keep a convenience reference for backwards compatibility
        self.client: Client = initial_client
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def _get_client_for_schema(self, schema: Optional[str]) -> Client:
        """Return a supabase client for the given schema, creating and caching it if needed."""
        schema_key = schema if schema is not None else None
        if schema_key in self._clients:
            return self._clients[schema_key]

        if schema:
            client = create_client(self.url, self.key, options=ClientOptions(schema=schema))
        else:
            client = create_client(self.url, self.key)

        self._clients[schema_key] = client
        return client
    
    def create(self, table: str, data: Union[Dict, List[Dict]], schema: Optional[str] = None) -> Dict:
        """Tạo records mới"""
        try:
            client = self._get_client_for_schema(schema or self.default_schema)
            result = client.table(table).insert(data).execute()
            self.logger.info(f"Tạo thành công {len(data)} records trong {table}")
            return {"success": True, "data": result.data}
        except Exception as e:
            self.logger.error(f"Lỗi tạo records: {str(e)}")
            return {"success": False, "error": str(e)}

    def read(self, table: str, columns: str = "*", filters: Dict = None, 
             order_by: str = None, limit: int = None, offset: int = None, schema: Optional[str] = None) -> Dict:
        """Đọc records"""
        try:
            client = self._get_client_for_schema(schema or self.default_schema)
            query = client.table(table).select(columns)
            
            # Áp dụng filters
            if filters:
                for key, value in filters.items():
                    query = query.eq(key, value)
            
            # Sắp xếp
            if order_by:
                query = query.order(order_by)
                
            # Phân trang
            if limit:
                query = query.limit(limit)
            if offset:
                query = query.offset(offset)
                
            result = query.execute()
            return {"success": True, "data": result.data, "count": len(result.data)}
        except Exception as e:
            self.logger.error(f"Lỗi đọc records: {str(e)}")
            return {"success": False, "error": str(e)}

    def call_rpc_function(self, function_name: str, params: Dict = None, schema: Optional[str] = None) -> Dict:
        try:
            client = self._get_client_for_schema(schema or self.default_schema)
            result = client.rpc(function_name, params).execute()
            return {"success": True, "data": result.data}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def update(self, table: str, data: Dict, filters: Dict, schema: Optional[str] = None) -> Dict:
        """Cập nhật records"""
        try:
            # allow optional schema via filters dict key '_schema' or via explicit param in future
            client = self._get_client_for_schema(schema or self.default_schema)
            query = client.table(table).update(data)
            
            # Áp dụng filters
            for key, value in filters.items():
                query = query.eq(key, value)
                
            result = query.execute()
            self.logger.info(f"Cập nhật thành công trong {table}")
            return {"success": True, "data": result.data}
        except Exception as e:
            self.logger.error(f"Lỗi cập nhật: {str(e)}")
            return {"success": False, "error": str(e)}

    def delete(self, table: str, filters: Dict) -> Dict:
        """Xóa records"""
        try:
            client = self._get_client_for_schema(None)
            query = self.client.table(table).delete()
            
            # Áp dụng filters
            for key, value in filters.items():
                query = query.eq(key, value)
                
            result = query.execute()
            self.logger.info(f"Xóa thành công trong {table}")
            return {"success": True, "data": result.data}
        except Exception as e:
            self.logger.error(f"Lỗi xóa: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def upsert(self, table: str, data: Union[Dict, List[Dict]], 
            on_conflict: str = None, schema: Optional[str] = None) -> Dict:
        """Upsert (insert or update)"""
        client = self._get_client_for_schema(schema or self.default_schema)
        upsert_kwargs = {}
        if on_conflict:
            upsert_kwargs["on_conflict"] = on_conflict
        query = client.table(table).upsert(data, **upsert_kwargs)
        result = query.execute()
        self.logger.info(f"Upsert thành công trong {table}")
        return {"success": True, "data": result.data}


    def batch_insert(self, table: str, data_list: List[Dict], 
                    batch_size: int = 1000) -> Dict:
        """Chèn dữ liệu theo batch"""
        try:
            results = []
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                client = self._get_client_for_schema(None)
                result = client.table(table).insert(batch).execute()
                results.extend(result.data)
            
            self.logger.info(f"Batch insert {len(results)} records")
            return {"success": True, "data": results}
        except Exception as e:
            self.logger.error(f"Lỗi batch insert: {str(e)}")
            return {"success": False, "error": str(e)}

    def query_with_conditions(self, table: str, conditions: Dict, schema: Optional[str] = None) -> Dict:
        """Query với nhiều điều kiện"""
        try:
            client = self._get_client_for_schema(schema or self.default_schema)
            query = client.table(table).select("*")
            
            for key, condition in conditions.items():
                if isinstance(condition, dict):
                    if "eq" in condition:
                        query = query.eq(key, condition["eq"])
                    elif "neq" in condition:
                        query = query.neq(key, condition["neq"])
                    elif "gt" in condition:
                        query = query.gt(key, condition["gt"])
                    elif "gte" in condition:
                        query = query.gte(key, condition["gte"])
                    elif "lt" in condition:
                        query = query.lt(key, condition["lt"])
                    elif "lte" in condition:
                        query = query.lte(key, condition["lte"])
                    elif "like" in condition:
                        query = query.like(key, condition["like"])
                    elif "in" in condition:
                        query = query.in_(key, condition["in"])
                else:
                    query = query.eq(key, condition)
            
            result = query.execute()
            return {"success": True, "data": result.data}
        except Exception as e:
            self.logger.error(f"Lỗi query: {str(e)}")
            return {"success": False, "error": str(e)}

    def count_records(self, table: str, filters: Dict = None) -> Dict:
        """Đếm số lượng records"""
        try:
            client = self._get_client_for_schema(None)
            query = client.table(table).select("*", count="exact")
            
            if filters:
                for key, value in filters.items():
                    query = query.eq(key, value)
            
            result = query.execute()
            return {"success": True, "count": result.count}
        except Exception as e:
            self.logger.error(f"Lỗi đếm records: {str(e)}")
            return {"success": False, "error": str(e)}

    def get_paginated_data(self, table: str, page: int = 1, 
                          page_size: int = 100, columns: str = "*") -> Dict:
        """Lấy dữ liệu phân trang"""
        try:
            offset = (page - 1) * page_size
            client = self._get_client_for_schema(None)
            result = client.table(table).select(columns)\
                .range(offset, offset + page_size - 1).execute()
            
            return {
                "success": True, 
                "data": result.data, 
                "page": page, 
                "page_size": page_size,
                "total": len(result.data)
            }
        except Exception as e:
            self.logger.error(f"Lỗi phân trang: {str(e)}")
            return {"success": False, "error": str(e)}

    def test_connection(self) -> Dict:
        """Kiểm tra kết nối"""
        try:
            client = self._get_client_for_schema(None)
            result = client.table("_temp_connection_test")\
                .select("*").limit(1).execute()
            return {"success": True, "message": "Kết nối thành công"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_table_info(self, table: str) -> Dict:
        """Lấy thông tin về bảng"""
        try:
            # Lấy sample data
            client = self._get_client_for_schema(None)
            sample_data = client.table(table).select("*").limit(1).execute()
            
            # Lấy schema (nếu cần)
            schema_info = {
                "sample_record": sample_data.data[0] if sample_data.data else None,
                "table_name": table
            }
            
            return {"success": True, "data": schema_info}
        except Exception as e:
            self.logger.error(f"Lỗi lấy thông tin bảng: {str(e)}")
            return {"success": False, "error": str(e)}