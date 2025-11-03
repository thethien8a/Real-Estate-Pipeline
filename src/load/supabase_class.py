
import os
import logging
from typing import List, Dict, Any, Optional, Union
from supabase import create_client, Client

class SupabaseManager:
    def __init__(self, url: str = None, key: str = None):
        """
        Khởi tạo Supabase Manager
        Args:
            url: Supabase URL
            key: Supabase API Key (khuyến nghị sb_secret_... cho pipeline)
        """
        self.url = os.environ.get("SUPABASE_URL")
        self.key = os.environ.get("SUPABASE_KEY")
        
        if not self.url or not self.key:
            raise ValueError("Supabase URL và Key là bắt buộc")
            
        self.client: Client = create_client(self.url, self.key)
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def create(self, table: str, data: Union[Dict, List[Dict]]) -> Dict:
        """Tạo records mới"""
        try:
            result = self.client.table(table).insert(data).execute()
            self.logger.info(f"Tạo thành công {len(data)} records trong {table}")
            return {"success": True, "data": result.data}
        except Exception as e:
            self.logger.error(f"Lỗi tạo records: {str(e)}")
            return {"success": False, "error": str(e)}

    def read(self, table: str, columns: str = "*", filters: Dict = None, 
             order_by: str = None, limit: int = None, offset: int = None) -> Dict:
        """Đọc records"""
        try:
            query = self.client.table(table).select(columns)
            
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

    def update(self, table: str, data: Dict, filters: Dict) -> Dict:
        """Cập nhật records"""
        try:
            query = self.client.table(table).update(data)
            
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
               on_conflict: str = None) -> Dict:
        """Upsert (insert or update)"""
        try:
            query = self.client.table(table).upsert(data)
            if on_conflict:
                query = query.on_conflict(on_conflict)
            result = query.execute()
            self.logger.info(f"Upsert thành công trong {table}")
            return {"success": True, "data": result.data}
        except Exception as e:
            self.logger.error(f"Lỗi upsert: {str(e)}")
            return {"success": False, "error": str(e)}

    def batch_insert(self, table: str, data_list: List[Dict], 
                    batch_size: int = 1000) -> Dict:
        """Chèn dữ liệu theo batch"""
        try:
            results = []
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                result = self.client.table(table).insert(batch).execute()
                results.extend(result.data)
            
            self.logger.info(f"Batch insert {len(results)} records")
            return {"success": True, "data": results}
        except Exception as e:
            self.logger.error(f"Lỗi batch insert: {str(e)}")
            return {"success": False, "error": str(e)}

    def query_with_conditions(self, table: str, conditions: Dict) -> Dict:
        """Query với nhiều điều kiện"""
        try:
            query = self.client.table(table).select("*")
            
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
            query = self.client.table(table).select("*", count="exact")
            
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
            result = self.client.table(table).select(columns)\
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
            result = self.client.table("_temp_connection_test")\
                .select("*").limit(1).execute()
            return {"success": True, "message": "Kết nối thành công"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_table_info(self, table: str) -> Dict:
        """Lấy thông tin về bảng"""
        try:
            # Lấy sample data
            sample_data = self.client.table(table).select("*").limit(1).execute()
            
            # Lấy schema (nếu cần)
            schema_info = {
                "sample_record": sample_data.data[0] if sample_data.data else None,
                "table_name": table
            }
            
            return {"success": True, "data": schema_info}
        except Exception as e:
            self.logger.error(f"Lỗi lấy thông tin bảng: {str(e)}")
            return {"success": False, "error": str(e)}