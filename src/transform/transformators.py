import pandas as pd
import re
import logging
import unicodedata
from datetime import datetime, timezone, date
logger = logging.getLogger(__name__)

class Transformators:
    STATUS_STAGE_PATTERNS = [
        ('Đã bàn giao', [
            'da ban giao',
            'da co so hong',
            'da ban giao toa',
            'da ban giao thap',
            'da ban giao giai doan',
            'da ban giao phan khu',
            'da xay dung hoan thien'
        ]),
        ('Bàn giao', [
            'ban giao nam',
            'ban giao thang',
            'ban giao quy',
            'ban giao vao',
            'ban giao ngay',
            'ban giao toa',
            'ban giao toan',
            'ban giao giai doan',
            'ban giao trong',
            'sap ban giao',
            'chuan bi ban giao',
            'dang ban giao'
        ]),
        ('Đang bán', [
            'dang ban',
            'dang mo ban',
            'ban het',
            'ban toan bo',
            'ban gio hang',
            'ban nhung can cuoi',
            'dang ban phan khu',
            'dang ban toa'
        ]),
        ('Mở bán', [
            'mo ban',
            'ra mat thap',
            'ra mat du an',
            'mo ban dot',
            'mo ban giai doan',
            'mo ban gio hang',
            'ra mat khu'
        ]),
        ('Nhận booking', [
            'nhan booking',
            'nhan dat cho',
            'giu cho',
            'dat cho',
            'nhan giu cho'
        ]),
        ('Cất nóc', [
            'cat noc',
            'sap cat noc'
        ]),
        ('Khởi công', [
            'khoi cong',
            'dong tho'
        ]),
        ('thi công', [
            'thi cong',
            'thi cong tro lai',
            'dang thi cong'
        ])
    ]

    PREPARATION_KEYWORDS = [
        'du kien',
        'chuan bi',
        'ra mat trong',
        'dang cap nhat',
        'sap ra mat',
        'sap mo ban',
        'sap khoi cong',
        'sap trien khai'
    ]

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
            'khu_vuc_cu_the': 'Không xác định',
            'thon_to_dan_pho': 'Không xác định',
            'phuong_xa': 'Không xác định',
            'quan_huyen': 'Không xác định',
            'tinh_thanh_pho': 'Không xác định',
        }
        
        if not address or address.strip() == '':
            raise ValueError("Address is required")
        
        # Tách theo dấu phẩy
        parts = [p.strip().title() for p in address.split(',')]
        num_parts = len(parts)
        
        if num_parts <= 2 or num_parts > 5:
            raise ValueError("Invalid address format")
        
        if any(char.isdigit() for char in parts[-1]):
            raise ValueError("Tỉnh/Thành phố không được chứa số.")

        # Clean tinh_thanh_pho
        tinh_thanh_pho = parts[-1].replace(".", "").lower().strip()
        tinh_thanh_pho = tinh_thanh_pho.replace("thành phố", "").replace("tỉnh", "").replace("tp", "")
        tinh_thanh_pho = tinh_thanh_pho.strip().title()

        if tinh_thanh_pho == "":
            tinh_thanh_pho = "Không xác định"
        elif tinh_thanh_pho.contains("Quận"):
            raise ValueError("Quận không được chứa trong tỉnh/thành phố")

        result['tinh_thanh_pho'] = tinh_thanh_pho
        result['quan_huyen'] = parts[-2]
        result['phuong_xa'] = parts[-3]
        
        # Xử lý các trường hợp đặc biệt
        if num_parts == 4:
            # TH1: "Đường Nguyễn Văn Linh, Liên Hà, Đông Anh, Hà Nội"
            if 'đường' in parts[0].lower():
                result['thon_to_dan_pho'] = parts[0]
                result["khu_vuc_cu_the"] = "Không xác định"
            # TH2: "Vinhomes Golden City, Hòa Nghĩa, Dương Kinh, Hà Nội"
            else:
                result['khu_vuc_cu_the'] = parts[0]
                result["thon_to_dan_pho"] = "Không xác định"

        elif num_parts == 5:
            # "Nhà An Khê, Lỗ Khê, Liên Hà, Đông Anh, Hà Nội"
            result['khu_vuc_cu_the'] = parts[0]
            result['thon_to_dan_pho'] = parts[1]
                
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
        if not price or not area_m2 or area_m2 <= 0:
            return -1
            
        price_str = str(price).lower().strip()
        
        # Thỏa thuận
        if 'thỏa thuận' in price_str or 'thoa thuan' in price_str:
            return -1
        
        # Thay dấu phẩy thành dấu chấm
        price_str = price_str.replace(',', '.')
        
        # Trích xuất số
        number = re.search(r'[\d.]+', price_str)

        if not number:
            raise ValueError(f"Price '{price}' không phù hợp")
        
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
            raise ValueError(f"Price '{price}' không phù hợp")
    
    @staticmethod
    def clean_area(area: str) -> float  :
        """
        Chuyển đổi area sang area_m2
        VD: "50.5m²" → 50.5
        """
        if not area:
            raise ValueError("Area is required")
            
        area_str = area.lower().strip()
        
        # Loại bỏ m², m2
        area_str = area_str.replace('m²', '').replace('m2', '').strip()
        
        # Thay dấu phẩy thành dấu chấm
        area_str = area_str.replace('.', '').replace(',', '.')
        
        # Chuyển thành số
        value = float(area_str)
        return round(value, 2)
    
    @staticmethod
    def clean_direction(direction: str):
        """Chuẩn hóa hướng nhà/ban công"""
        if not direction:
            return 'Không xác định'
        if '-' in direction:
            direction = direction.replace('-', ' ')
            # Loại bỏ khoảng trắng thừa
            direction = ' '.join(direction.split())
        return str(direction).strip().title()
    
    @staticmethod
    def clean_facade(facade: str):
        """
        Xử lý mặt tiền
        VD: "5m" → 5.0, "5,5m" → 5.5
        """
        if not facade:
            return -1
        
        # Thay dấu phẩy thành dấu chấm
        facade = facade.replace(',', '.')
        
        # Loại bỏ 'm'
        facade = facade.replace('m', '').strip()
        
        # Chuyển thành số
        value = float(facade)
        return round(value, 2)
    
    @staticmethod
    def clean_legal(legal: str):
        """
        Tách legal thành 3 cột:
        - have_red_book: có sổ đỏ
        - have_pink_book: có sổ hồng  
        - have_sale_contract: có hợp đồng mua bán
        """
        result = {
            'have_red_book': 0,
            'have_pink_book': 0,
            'have_sale_contract': 0,
            'have_agreement_document': 0,
        }
        
        if not legal:
            return result
            
        legal = legal.lower()
        
        has_any_legal = 0
        
        # Kiểm tra sổ đỏ
        if any(keyword in legal for keyword in ['sổ đỏ', 'so do']):
            result['have_red_book'] = 1
            has_any_legal = 1
      
        # Kiểm tra sổ hồng
        if any(keyword in legal for keyword in ['sổ hồng', 'so hong']):
            result['have_pink_book'] = 1
            has_any_legal = 1
            
        # Kiểm tra hợp đồng mua bán
        if any(keyword in legal for keyword in ['hợp đồng mua bán', 'hop dong mua ban', 'hđmb', 'hdmb']):
            result['have_sale_contract'] = 1
            has_any_legal = 1
            
        # Kiểm tra văn bản thỏa thuận
        if any(keyword in legal for keyword in ['văn bản', 'van ban', 'vbtt']):
            result['have_agreement_document'] = 1
            has_any_legal = 1
        
        # Đánh dấu lỗi nếu không có loại giấy tờ nào
        if not has_any_legal:
            raise ValueError("Giấy tờ cần xử lý thủ công")
            
        return result
    
    @staticmethod
    def clean_furniture(furniture: str):
        """
        Tách furniture thành 2 cột:
        - furniture_type: full/basic
        - has_premium_furniture: có nội thất cao cấp/Unknown
        """
        result = {
            'furniture_type': 'Không xác định',
            'has_premium_furniture': 0
        }
        
        if not furniture:
            return result
            
        furniture = furniture.lower()

        has_full_furniture = 0
        has_basic_furniture = 0
        has_premium_furniture = 0
        
        # Kiểm tra đầy đủ nội thất
        if any(keyword in furniture for keyword in ['đầy đủ', 'full', 'hoàn thiện']):
            has_full_furniture = 1
        if any(keyword in furniture for keyword in ['cơ bản', 'basic', 'đơn giản']):
            has_basic_furniture = 1
        
        if has_full_furniture == 1 and has_basic_furniture == 1: # TH này là "đầy đủ nội thất cơ bản"
            result['furniture_type'] = 'basic'
        elif has_full_furniture == 1: # TH này là "đầy đủ nội thất"
            result['furniture_type'] = 'full'
        elif has_basic_furniture == 1: # TH này là "cơ bản"
            result['furniture_type'] = 'basic'
            
        # Có nội thất cao cấp không
        if any(keyword in furniture for keyword in ['cao cấp', 'sang trọng', 'premium', 'luxury']):
            result['has_premium_furniture'] = 1
            has_premium_furniture = 1
        else:
            result['has_premium_furniture'] = 0
        
        if has_full_furniture == 0 and has_basic_furniture == 0 and has_premium_furniture == 0:
            raise ValueError("Nội thất cần xử lý thủ công")
            
        return result
    
    @staticmethod
    def clean_number_field(value: str):
        """
        Xử lý các trường số (số phòng ngủ, phòng tắm, tầng)
        VD: "3 phòng" → 3
        """
        if not value:
            return -1
        
        # Trích xuất số
        number = re.search(r'\d+', value)
        if number:
            return int(number.group())
        else:
            raise ValueError(f"Error cleaning number field '{value}'")

    @staticmethod
    def clean_date_field(value):
        """
        Chuẩn hóa ngày dạng '1/12/2025' (dd/mm/yyyy) sang ISO string.
        Nếu dữ liệu đã ở ISO format thì trả về nguyên trạng.
        """
        if value is None:
            return None

        if isinstance(value, datetime):
            return value.isoformat()

        if isinstance(value, date):
            return value.isoformat()

        if not isinstance(value, str):
            logger.warning("Giá trị ngày không hợp lệ: %s", value)
            return None

        value_str = value.strip()
        if not value_str:
            return None

        # Dữ liệu đã ở dạng ISO (ví dụ 2025-12-01 hoặc có 'T')
        if "-" in value_str or "T" in value_str:
            try:
                normalized = value_str.replace("Z", "+00:00")
                parsed = datetime.fromisoformat(normalized)
                # Giữ nguyên kiểu gốc (date hay datetime)
                return parsed.isoformat() if "T" in normalized or ":" in normalized else parsed.date().isoformat()
            except ValueError:
                logger.warning("Không parse được ISO date: %s", value)
                return None

        # Mặc định dữ liệu dạng dd/mm/yyyy (có thể thiếu số 0 bên trái)
        try:
            parsed = datetime.strptime(value_str, "%d/%m/%Y")
            return parsed.date().isoformat()
        except ValueError:
            logger.warning("Không parse được ngày dd/mm/yyyy: %s", value)
            return None
    
    @staticmethod
    def clean_way_in(way_in: str):
        """
        Xử lý lối vào
        VD: "5m" → 5.0, '"5"' → 5.0
        """
        if not way_in:
            return -1
        
        way_in = way_in.lower()

        if '"' in way_in:
            way_in = way_in.replace('"', '')
        if "'" in way_in:
            way_in = way_in.replace("'", '')

        if 'm' in way_in:
            way_in = way_in.replace('m', '')

        if ',' in way_in:
            way_in = way_in.replace(',', '.')
                    
        way_in = way_in.strip()
        
        # Chuyển thành số
        value = float(way_in)
        return round(value, 2)
    
    @staticmethod
    def clean_project_name(project_name):
        """Chuẩn hóa tên dự án"""
        if not project_name or str(project_name).strip() == '':
            return 'Không xác định'
        cleaned = str(project_name).strip() 
        cleaned = ' '.join(cleaned.split()) # Chuẩn hóa khoảng trắng
        cleaned = cleaned.title() # Viết hoa chữ cái đầu mỗi từ
        return cleaned
    
    @staticmethod
    def _normalize_project_status_text(project_status: str) -> str:
        """Chuẩn hóa text project_status phục vụ phân loại stage"""
        if not project_status:
            return ''
        normalized = unicodedata.normalize('NFD', str(project_status))
        normalized = ''.join(ch for ch in normalized if unicodedata.category(ch) != 'Mn')
        normalized = normalized.replace('Đ', 'D').replace('đ', 'd')
        normalized = normalized.lower()
        normalized = re.sub(r'[^a-z0-9/ ]', ' ', normalized)
        normalized = ' '.join(normalized.split())
        return normalized
    
    @staticmethod
    def extract_project_status_stage(project_status: str) -> str:
        """
        Phân loại project_status thành stage tiếng Việt
        """
        if not project_status or project_status.strip() == '' or project_status == '(blank)':
            return 'Không xác định'
        
        normalized = Transformators._normalize_project_status_text(project_status)
        if not normalized or normalized == 'khong xac dinh':
            return 'Không xác định'
        
        # Ưu tiên các trạng thái chuẩn bị để tránh trùng với các stage khác
        if any(keyword in normalized for keyword in Transformators.PREPARATION_KEYWORDS):
            return 'Chuẩn bị'
        
        for stage_label, keywords in Transformators.STATUS_STAGE_PATTERNS:
            for keyword in keywords:
                if keyword in normalized:
                    return stage_label
        
        return 'Khác'
    
    @staticmethod
    def clean_project_investor(project_investor):
        """Chuẩn hóa chủ đầu tư - viết hoa tất cả chữ đầu"""
        if not project_investor or str(project_investor).strip() == '':
            return 'Không xác định'
        cleaned = str(project_investor).strip() 
        cleaned = ' '.join(cleaned.split()) 
        cleaned = cleaned.title()
        return cleaned
    
    @staticmethod
    def extract_property_type(subpage_url: str):
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
        
        if not subpage_url:
            raise ValueError("URL rỗng")
            
        url_lower = subpage_url.lower()
        
        for pattern, prop_type in property_types.items():
            if pattern in url_lower:
                return prop_type
                
        # Không tìm thấy pattern nào
        raise ValueError(f"URL không khớp với pattern nào: {subpage_url}")
    
    @staticmethod
    def transform_row(row):
        """
        Transform toàn bộ row từ staging sang silver
        """
        cleaned_row = {}
        
        # 1. Page number
        cleaned_row["main_page_url"] = row.get('main_page_url')
        cleaned_row['page_number'] = Transformators.extract_page_number(row.get('main_page_url'))
        
        subpage_url = row.get('subpage_url')
        title = row.get('title')
            
        cleaned_row['subpage_url'] = subpage_url
        cleaned_row['title'] = title
        # 2. Parse address
        address_result = Transformators.parse_address(row.get('address'))
        cleaned_row['specific_area'] = address_result['khu_vuc_cu_the']
        cleaned_row['hamlet_neighborhood'] = address_result['thon_to_dan_pho']
        cleaned_row['ward_commune'] = address_result['phuong_xa']
        cleaned_row['district'] = address_result['quan_huyen']
        cleaned_row['province_city'] = address_result['tinh_thanh_pho']
        
        # 3. Clean area trước 
        area_m2 = Transformators.clean_area(row.get('area'))
        cleaned_row['area_m2'] = area_m2
        
        # 4. Clean price (cần area_m2)
        million_per_m2 = Transformators.clean_price(row.get('price'), area_m2)
        cleaned_row['million_per_m2'] = million_per_m2
        
        # 5. Directions
        cleaned_row['house_direction'] = Transformators.clean_direction(row.get('house_direction'))
        cleaned_row['balcony_direction'] = Transformators.clean_direction(row.get('balcony_direction'))
        
        # 6. Facade
        facade = Transformators.clean_facade(row.get('facade'))
        cleaned_row['facade'] = facade
        
        # 7. Legal
        legal_result = Transformators.clean_legal(row.get('legal'))
        cleaned_row['have_red_book'] = legal_result['have_red_book']
        cleaned_row['have_pink_book'] = legal_result['have_pink_book']
        cleaned_row['have_sale_contract'] = legal_result['have_sale_contract']
        cleaned_row['have_agreement_document'] = legal_result['have_agreement_document']
        
        # 8. Furniture
        furniture_result = Transformators.clean_furniture(row.get('furniture'))
        cleaned_row['furniture_type'] = furniture_result['furniture_type']
        cleaned_row['has_premium_furniture'] = furniture_result['has_premium_furniture']
        
        # 9. Number fields
        cleaned_row['number_bedroom'] = Transformators.clean_number_field(row.get('number_bedroom'))
        cleaned_row['number_bathroom'] = Transformators.clean_number_field(row.get('number_bathroom'))
        cleaned_row['number_floor'] = Transformators.clean_number_field(row.get('number_floor'))
        
        # 10. Way in
        way_in_m = Transformators.clean_way_in(row.get('way_in'))
        cleaned_row['way_in_m'] = way_in_m
        
        # 11. Project fields
        cleaned_row['project_name'] = Transformators.clean_project_name(row.get('project_name'))
        cleaned_row['project_status_stage'] = Transformators.extract_project_status_stage(row.get('project_status'))      
        cleaned_row['project_investor'] = Transformators.clean_project_investor(row.get('project_investor'))
        
        # Metadata từ staging

        cleaned_row['post_id'] = row.get('post_id')
        cleaned_row['post_start_time'] = Transformators.clean_date_field(row.get('post_start_time'))
        cleaned_row['post_end_time'] = Transformators.clean_date_field(row.get('post_end_time'))
        cleaned_row['post_type'] = row.get('post_type')
        
        cleaned_row['source'] = row.get('source')
        cleaned_row['store_staging_at'] = row.get('created_at')
        cleaned_row["transformed_at"] = datetime.now(timezone.utc).isoformat()
        
        # 12. Property type từ URL
        cleaned_row['loai_bat_dong_san'] = Transformators.extract_property_type(row.get('subpage_url'))
        
        return cleaned_row
    