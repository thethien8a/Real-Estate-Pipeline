import pandas as pd
import re
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class Transformators:
    def __init__(self):
        pass
    
    @staticmethod
    def extract_page_number(main_page_url):
        """
        Tách số trang từ main_page_url
        VD: https://batdongsan.com.vn/nha-dat-ban/p1 → 1
        """
        match = re.search(r'/p(\d+)', main_page_url)
        if match:
            return int(match.group(1))
        
    @staticmethod
    def parse_address(address):
        """
        Tách address thành 5 phần: 
        Khu vực cụ thể, Thôn/Tổ dân phố, Phường/Xã, Quận/Huyện, Tỉnh/Thành phố
        """
        result = {
            'khu_vuc_cu_the': 'không xác định',
            'thon_to_dan_pho': 'không xác định',
            'phuong_xa': 'không xác định',
            'quan_huyen': 'không xác định',
            'tinh_thanh_pho': 'không xác định',
            'is_error': False
        }
        
        try:
            if not address or address.strip() == '':
                result['is_error'] = True
                return result
            
            # Tách theo dấu phẩy
            parts = [p.strip().title() for p in address.split(',')]
            num_parts = len(parts)
            
            if num_parts <= 2 or num_parts > 5:
                result['is_error'] = True
                return result
            
            result['tinh_thanh_pho'] = parts[-1]
            result['quan_huyen'] = parts[-2]
            result['phuong_xa'] = parts[-3]
            
            # Xử lý các trường hợp đặc biệt
            if num_parts == 4:
                # TH1: "Đường Nguyễn Văn Linh, Liên Hà, Đông Anh, Hà Nội"
                if 'đường' in parts[0].lower():
                    result['thon_to_dan_pho'] = parts[0]
                # TH2: "Vinhomes Golden City, Hòa Nghĩa, Dương Kinh, Hà Nội"
                else:
                    result['khu_vuc_cu_the'] = parts[0]

            elif num_parts == 5:
                # "Nhà An Khê, Lỗ Khê, Liên Hà, Đông Anh, Hà Nội"
                result['khu_vuc_cu_the'] = parts[0]
                result['thon_to_dan_pho'] = parts[1]
                
                
        except Exception as e:
            logger.error(f"Error parsing address '{address}': {e}")
            result['is_error'] = True
            
        return result
    
    @staticmethod
    def clean_price(price, area_m2):
        """
        Chuyển đổi price sang million_per_m2 (triệu đồng/m²)
        VD: 
        - "1.5 tỷ" → 1500 triệu → 1500/area_m2
        - "50 triệu" → 50/area_m2
        - "5 triệu/m²" → 5
        """
        try:
            if not price or not area_m2 or area_m2 <= 0:
                return None
                
            price_str = str(price).lower().strip()
            
            # Thỏa thuận
            if 'thỏa thuận' in price_str or 'thoả thuận' in price_str:
                return None
            
            # Thay dấu phẩy thành dấu chấm
            price_str = price_str.replace(',', '.')
            
            # Trích xuất số
            number = re.search(r'[\d.]+', price_str)
            if not number:
                return None
            value = float(number.group())
            
            # Xử lý đơn vị
            if 'tỷ' in price_str:
                # Chuyển tỷ → triệu
                value_in_million = value * 1000
                return round(value_in_million / area_m2, 2)
            elif 'triệu/m' in price_str:
                # Đã là triệu/m²
                return round(value, 2)
            elif 'triệu' in price_str:
                # Tổng giá triệu → chia cho diện tích
                return round(value / area_m2, 2)
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error cleaning price '{price}': {e}")
            return None
    
    @staticmethod
    def clean_area(area):
        """
        Chuyển đổi area sang area_m2
        VD: "50.5m²" → 50.5
        """
        try:
            if not area:
                return -1
                
            area_str = str(area).lower().strip()
            
            # Loại bỏ m², m2
            area_str = area_str.replace('m²', '').replace('m2', '').strip()
            
            # Thay dấu phẩy thành dấu chấm
            area_str = area_str.replace('.', '').replace(',', '.')
            
            # Chuyển thành số
            value = float(area_str)
            return round(value, 2)
            
        except Exception as e:
            logger.error(f"Error cleaning area '{area}': {e}")
            return -1
    
    @staticmethod
    def clean_direction(direction):
        """Chuẩn hóa hướng nhà/ban công"""
        if not direction or str(direction).strip() == '':
            return 'không xác định'
        return str(direction).strip().title()
    
    @staticmethod
    def clean_facade(facade):
        """
        Xử lý mặt tiền
        VD: "5m" → 5.0, "5,5m" → 5.5
        """
        try:
            if not facade:
                return -1
                
            facade_str = str(facade).lower().strip()
            
            # Loại bỏ 'm'
            facade_str = facade_str.replace('m', '').strip()
            
            # Thay dấu phẩy thành dấu chấm
            facade_str = facade_str.replace(',', '.')
            
            # Chuyển thành số
            value = float(facade_str)
            return round(value, 2)
            
        except Exception as e:
            logger.error(f"Error cleaning facade '{facade}': {e}")
            return -1
    
    @staticmethod
    def clean_legal(legal):
        """
        Tách legal thành 3 cột:
        - have_red_book: có sổ đỏ
        - have_pink_book: có sổ hồng  
        - have_sale_contract: có hợp đồng mua bán
        """
        result = {
            'have_red_book': 0,
            'have_pink_book': 0,
            'have_sale_contract': 0
        }
        
        try:
            if not legal:
                return result
                
            legal_str = str(legal).lower()
            
            if 'sổ đỏ' in legal_str or 'so do' in legal_str:
                result['have_red_book'] = 1
                
            if 'sổ hồng' in legal_str or 'so hong' in legal_str:
                result['have_pink_book'] = 1
                
            if 'hợp đồng mua bán' in legal_str or 'hop dong mua ban' in legal_str:
                result['have_sale_contract'] = 1
                
        except Exception as e:
            logger.error(f"Error cleaning legal '{legal}': {e}")
            
        return result
    
    @staticmethod
    def clean_furniture(furniture):
        """
        Tách furniture thành 2 cột:
        - have_full_furniture: có đầy đủ nội thất
        - furniture_type: loại nội thất (basic, premium)
        """
        result = {
            'have_full_furniture': 0,
            'furniture_type': 'không xác định'
        }
        
        try:
            if not furniture:
                return result
                
            furniture_str = str(furniture).lower()
            
            # Kiểm tra đầy đủ nội thất
            if any(keyword in furniture_str for keyword in ['đầy đủ', 'full', 'hoàn thiện']):
                result['have_full_furniture'] = 1
                
            # Phân loại loại nội thất
            if any(keyword in furniture_str for keyword in ['cao cấp', 'sang trọng', 'premium', 'luxury']):
                result['furniture_type'] = 'premium'
            elif any(keyword in furniture_str for keyword in ['cơ bản', 'basic', 'đơn giản']):
                result['furniture_type'] = 'basic'
            elif result['have_full_furniture'] == 1:
                result['furniture_type'] = 'full'
                
        except Exception as e:
            logger.error(f"Error cleaning furniture '{furniture}': {e}")
            
        return result
    
    @staticmethod
    def clean_number_field(value, unit_to_remove=''):
        """
        Xử lý các trường số (số phòng ngủ, phòng tắm, tầng)
        VD: "3 phòng" → 3
        """
        try:
            if not value:
                return -1
                
            value_str = str(value).lower().strip()
            
            # Loại bỏ đơn vị
            if unit_to_remove:
                value_str = value_str.replace(unit_to_remove.lower(), '').strip()
            
            # Trích xuất số
            number = re.search(r'\d+', value_str)
            if number:
                return int(number.group())
            return -1
            
        except Exception as e:
            logger.error(f"Error cleaning number field '{value}': {e}")
            return -1
    
    @staticmethod
    def clean_way_in(way_in):
        """
        Xử lý lối vào
        VD: "5m" → 5.0, '"5"' → 5.0
        """
        try:
            if not way_in:
                return -1
                
            way_in_str = str(way_in).strip()
            
            # Loại bỏ dấu nháy kép
            way_in_str = way_in_str.replace('"', '').replace("'", '')
            
            # Loại bỏ 'm'
            way_in_str = way_in_str.replace('m', '').strip()
            
            # Thay dấu phẩy thành dấu chấm
            way_in_str = way_in_str.replace(',', '.')
            
            # Chuyển thành số
            value = float(way_in_str)
            return round(value, 2)
            
        except Exception as e:
            logger.error(f"Error cleaning way_in '{way_in}': {e}")
            return -1
    
    @staticmethod
    def clean_project_name(project_name):
        """Chuẩn hóa tên dự án"""
        if not project_name or str(project_name).strip() == '':
            return 'không xác định'
        return str(project_name).strip()
    
    @staticmethod
    def clean_project_status(project_status):
        """
        Transform project_status - Giữ nguyên giá trị hoặc xử lý thủ công
        Các trường hợp dài, phức tạp → đưa vào error_table
        """
        if not project_status or project_status.strip() == '' or project_status == '(blank)':
            return 'không xác định'
        
        # Nếu status quá dài (>100 ký tự) → cần xử lý thủ công
        if len(project_status) > 100:
            raise ValueError("Project status quá dài, cần xử lý thủ công")
        
        return str(project_status).strip()
    
    @staticmethod
    def clean_project_investor(project_investor):
        """Chuẩn hóa chủ đầu tư - viết hoa tất cả chữ đầu"""
        if not project_investor or str(project_investor).strip() == '':
            return 'không xác định'
        return str(project_investor).strip().title()
    
    @staticmethod
    def extract_property_type(subpage_url):
        """
        Tách loại bất động sản từ subpage_url
        """
        property_types = {
            'ban-nha-biet-thu': 'Nhà biệt thự',
            'ban-can-ho-chung-cu': 'Căn hộ chung cư',
            'ban-nha-rieng': 'Nhà riêng',
            'ban-dat-xa': 'Đất xã',
            'ban-dat-duong': 'Đất đường',
            'ban-dat-nen': 'Đất nền',
            'ban-condotel': 'Condotel',
            'ban-shophouse': 'Shophouse',
            'ban-loai-bat-dong-san-khac': 'Bất động sản khác',
            'ban-nha-mat-pho': 'Nhà mặt phố',
            'ban-dat-phuong': 'Đất phường',
            'ban-trang-trai': 'Trang trại'
        }
        
        try:
            if not subpage_url:
                raise ValueError("URL rỗng")
                
            url_lower = subpage_url.lower()
            
            for pattern, prop_type in property_types.items():
                if pattern in url_lower:
                    return prop_type
                    
            # Không tìm thấy pattern nào
            raise ValueError(f"URL không khớp với pattern nào: {subpage_url}")
            
        except Exception as e:
            logger.error(f"Error extracting property type from '{subpage_url}': {e}")
            raise
    
    @staticmethod
    def transform_row(row):
        """
        Transform toàn bộ row từ staging sang silver
        """
        try:
            cleaned_row = {}
            error_cols = []
            
            # 1. Page number
            cleaned_row['page_number'] = Transformators.extract_page_number(row.get('main_page_url'))
            
            # 2. Parse address
            address_result = Transformators.parse_address(row.get('address'))
            if address_result['is_error']:
                error_cols.append('address')
            cleaned_row['khu_vuc_cu_the'] = address_result['khu_vuc_cu_the']
            cleaned_row['thon_to_dan_pho'] = address_result['thon_to_dan_pho']
            cleaned_row['phuong_xa'] = address_result['phuong_xa']
            cleaned_row['quan_huyen'] = address_result['quan_huyen']
            cleaned_row['tinh_thanh_pho'] = address_result['tinh_thanh_pho']
            
            # 3. Clean area trước (cần cho việc tính price)
            area_m2 = Transformators.clean_area(row.get('area'))
            if area_m2 == -1:
                error_cols.append('area')
            cleaned_row['area_m2'] = area_m2
            
            # 4. Clean price (cần area_m2)
            million_per_m2 = Transformators.clean_price(row.get('price'), area_m2)
            if million_per_m2 is None and row.get('price'):
                error_cols.append('price')
            cleaned_row['million_per_m2'] = million_per_m2
            
            # 5. Directions
            cleaned_row['house_direction'] = Transformators.clean_direction(row.get('house_direction'))
            cleaned_row['balcony_direction'] = Transformators.clean_direction(row.get('balcony_direction'))
            
            # 6. Facade
            facade = Transformators.clean_facade(row.get('facade'))
            if facade == -1 and row.get('facade'):
                error_cols.append('facade')
            cleaned_row['facade'] = facade
            
            # 7. Legal
            legal_result = Transformators.clean_legal(row.get('legal'))
            cleaned_row.update(legal_result)
            
            # 8. Furniture
            furniture_result = Transformators.clean_furniture(row.get('furniture'))
            cleaned_row.update(furniture_result)
            
            # 9. Number fields
            cleaned_row['number_bedroom'] = Transformators.clean_number_field(row.get('number_bedroom'), 'phòng')
            if cleaned_row['number_bedroom'] == -1 and row.get('number_bedroom'):
                error_cols.append('number_bedroom')
                
            cleaned_row['number_bathroom'] = Transformators.clean_number_field(row.get('number_bathroom'), 'phòng')
            if cleaned_row['number_bathroom'] == -1 and row.get('number_bathroom'):
                error_cols.append('number_bathroom')
                
            cleaned_row['number_floor'] = Transformators.clean_number_field(row.get('number_floor'), 'tầng')
            if cleaned_row['number_floor'] == -1 and row.get('number_floor'):
                error_cols.append('number_floor')
            
            # 10. Way in
            way_in_m = Transformators.clean_way_in(row.get('way_in'))
            if way_in_m == -1 and row.get('way_in'):
                error_cols.append('way_in')
            cleaned_row['way_in_m'] = way_in_m
            
            # 11. Project fields
            cleaned_row['project_name'] = Transformators.clean_project_name(row.get('project_name'))
            
            try:
                cleaned_row['project_status'] = Transformators.clean_project_status(row.get('project_status'))
            except ValueError:
                error_cols.append('project_status')
                cleaned_row['project_status'] = 'không xác định'
            
            cleaned_row['project_investor'] = Transformators.clean_project_investor(row.get('project_investor'))
            
            # 12. Property type từ URL
            try:
                cleaned_row['loai_bat_dong_san'] = Transformators.extract_property_type(row.get('subpage_url'))
            except:
                error_cols.append('subpage_url')
                cleaned_row['loai_bat_dong_san'] = 'không xác định'
            
            # 13. Giữ nguyên các cột khác
            keep_cols = [
                'main_page_url', 'subpage_url', 'title', 
                'post_id', 'post_start_time', 'post_end_time', 'post_type',
                'source', 'crawled_at'
            ]
            for col in keep_cols:
                cleaned_row[col] = row.get(col, 'không xác định')
            
            # Nếu có lỗi → raise exception để đưa vào error_table
            if error_cols:
                raise ValueError(f"Lỗi tại các cột: {', '.join(error_cols)}")
            
            return cleaned_row, None
            
        except Exception as e:
            error_message = str(e)
            logger.error(f"Error transforming row: {error_message}")
            return None, error_message


if __name__ == "__main__":
    test_address = "Lỗ Khê, Đông Anh, Hà Nội"
    print(Transformators.parse_address(test_address))