import os
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from supabase_class import SupabaseManager
import pandas as pd
import math
from typing import Any
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class StagingLoader:
    def __init__(self, data_dir: str = None):        
        if data_dir is None:
            data_dir = Path(__file__).resolve().parents[2] / "data" / "raw"
        
        # SupabaseManager khởi tạo với schema mặc định 'bronze'
        self.supabase = SupabaseManager(default_schema="bronze")
        self.data_dir = Path(data_dir)
        # store table name without schema; schema sẽ dùng từ client mặc định
        self.processed_files_table = "processed_files_log"
        
    def extract_timestamp_from_filename(self, filename: str) -> Optional[datetime]:
        """
        Trích xuất timestamp từ tên file.
        Hỗ trợ định dạng: data_20241105_143022.csv
        """
        pattern = r'(\d{8})_(\d{6})'
        
        match = re.search(pattern, filename)
        if match:
            date_part, time_part = match.groups()            
            try:
                return datetime.strptime(f"{date_part}{time_part}", "%Y%m%d%H%M%S")
            except ValueError:
                return None
    
        # Fallback: sử dụng file modification time
        try:
            file_path = self.data_dir / filename
            return datetime.fromtimestamp(file_path.stat().st_mtime)
        except:
            return None
    
    def get_latest_processed_timestamp(self) -> Optional[datetime]:
        """Lấy timestamp của file đã xử lý lần cuối"""
        try:
            result = self.supabase.read(
                table=self.processed_files_table,
                columns="processed_at",
                order_by="processed_at desc",
                limit=1
            )
            
            if result["success"] and result["data"]:
                return datetime.fromisoformat(result["data"][0]["processed_at"])
            return None
        except Exception as e:
            logger.warning(f"Không thể lấy timestamp đã xử lý: {e}")
            return None
    
    def get_unprocessed_files(self) -> List[Tuple[Path, datetime]]:
        """Lấy danh sách file chưa được xử lý"""
        if not self.data_dir.exists():
            logger.error(f"Thư mục {self.data_dir} không tồn tại")
            return []
        
        latest_processed = self.get_latest_processed_timestamp()
        unprocessed_files = []
        
        for file_path in self.data_dir.glob("*"):
            if file_path.is_file():
                timestamp = self.extract_timestamp_from_filename(file_path.name)
                if timestamp and (not latest_processed or timestamp > latest_processed):
                    unprocessed_files.append((file_path, timestamp))
        
        # Sắp xếp theo timestamp tăng dần
        unprocessed_files.sort(key=lambda x: x[1])
        return unprocessed_files
    
    def log_processed_file(self, file_path: Path, timestamp: datetime, 
                          record_count: int, status: str = "success") -> bool:
        """Ghi log file đã xử lý"""
        try:
            log_data = {
                "file_name": file_path.name,
                "file_path": str(file_path),
                "file_timestamp": timestamp.isoformat(),
                "processed_at": datetime.now().isoformat(),
                "record_count": record_count,
                "status": status
            }
            
            result = self.supabase.create(self.processed_files_table, log_data)
            return result["success"]
        except Exception as e:
            logger.error(f"Lỗi khi ghi log processed file: {e}")
            return False
    
    def load_staging(self, data: List[Dict], batch_size: int = 1000) -> bool:
        """Tải dữ liệu vào staging table với batch processing"""
        try:
            if len(data) <= batch_size:
                result = self.supabase.create("staging", data)
                return result["success"]
            else:
                # Batch processing cho dữ liệu lớn
                result = self.supabase.batch_insert("staging", data, batch_size)
                return result["success"]
        except Exception as e:
            logger.error(f"Lỗi khi tải dữ liệu vào staging: {e}")
            return False
    
    def get_data_from_file(self, file_path: Path) -> Optional[List[Dict]]:
        """Đọc dữ liệu từ file"""
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                    df = pd.read_csv(file_path)
                    data = df.to_dict('records')
                    
                    # Xử lý NaN và các giá trị không hợp lệ trước khi trả về, biến chúng thành None 
                    return StagingLoader.clean_data_for_json(data)
            
        except Exception as e:
            logger.error(f"Lỗi khi đọc file {file_path}: {e}")
            return None
    
    def move_processed_file(self, file_path: Path, status: str = "success") -> bool:
        """Di chuyển file đã xử lý đến thư mục tương ứng"""
        try:
            processed_dir = self.data_dir.parent / "processed" if status == "success" else self.data_dir.parent / "error"
            processed_dir.mkdir(exist_ok=True)
            
            destination = processed_dir / file_path.name
            file_path.rename(destination)
            logger.info(f"Đã di chuyển {file_path.name} đến {processed_dir.name}")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi di chuyển file {file_path}: {e}")
            return False
    
    def process_latest_files(self) -> Dict[str, int]:
        """
        Hàm chính: xử lý tất cả file mới và tải vào staging
        Returns: Dict với thông tin kết quả
        """
        unprocessed_files = self.get_unprocessed_files()
        
        if not unprocessed_files:
            logger.info("Không có file mới để xử lý")
            return {"status": "no_files", "processed": 0, "failed": 0}
        
        processed_count = 0
        failed_count = 0
        
        for file_path, timestamp in unprocessed_files:
            logger.info(f"Đang xử lý file: {file_path.name}")
            
            # Đọc dữ liệu
            data = self.get_data_from_file(file_path)
            if not data:
                logger.error(f"Không thể đọc dữ liệu từ {file_path.name}")
                self.log_processed_file(file_path, timestamp, 0, "failed")
                self.move_processed_file(file_path, "error")
                failed_count += 1
                continue
            
            # Tải vào staging
            success = self.load_staging(data)
            
            if success:
                # Ghi log thành công
                self.log_processed_file(file_path, timestamp, len(data), "success")
                self.move_processed_file(file_path, "success")
                processed_count += 1
                logger.info(f"Đã xử lý thành công {file_path.name} với {len(data)} records")
            else:
                # Ghi log thất bại
                self.log_processed_file(file_path, timestamp, len(data), "failed")
                self.move_processed_file(file_path, "error")
                failed_count += 1
                logger.error(f"Xử lý thất bại {file_path.name}")
        
        return {
            "status": "completed",
            "processed": processed_count,
            "failed": failed_count,
            "total": processed_count + failed_count
        }
        
    @staticmethod
    def clean_value(value: Any) -> Any:
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return None 
        elif isinstance(value, dict):
            return {k: StagingLoader.clean_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [StagingLoader.clean_value(item) for item in value]
        return value

    @staticmethod
    def clean_data_for_json(data: List[Dict]) -> List[Dict]:
        """
        Clean data to remove NaN, inf and other non-JSON compliant values
        """

        
        cleaned_data = []
        for record in data:
            cleaned_record = {k: StagingLoader.clean_value(v) for k, v in record.items()}
            cleaned_data.append(cleaned_record)
        
        return cleaned_data

# Sử dụng
if __name__ == "__main__":
    loader = StagingLoader()
    result = loader.process_latest_files()
    logger.info(f"Kết quả: {result}")
