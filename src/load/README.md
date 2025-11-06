Nếu bạn muốn sử dụng project này (áp dụng khi clone về và làm việc với database của riêng bạn trên supabase (không phải database supabase của tôi)), bạn cần làm những công việc sau:

1. **Tạo file .env** với Supabase credentials:
   ```
   SUPABASE_URL=https://your-project-id.supabase.co
   SUPABASE_KEY=sb_secret_your_service_role_key_here
   ```

2. **Vào Supabase SQL Editor, chạy script setup:**

```sql
-- Tạo schema bronze
CREATE SCHEMA IF NOT EXISTS bronze;

-- Grant permissions cho service role
GRANT ALL ON SCHEMA bronze TO service_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO service_role;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bronze TO service_role;

-- Tạo bảng staging cho dữ liệu bất động sản
CREATE TABLE bronze.staging (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  main_page_url TEXT,           
  subpage_url TEXT,             
  title TEXT,                   
  address TEXT,                 
  price TEXT,                   
  area TEXT,                    
  house_direction TEXT,         
  balcony_direction TEXT,       
  facade TEXT,                  
  legal TEXT,                   
  furniture TEXT,               
  number_bedroom TEXT,          
  number_bathroom TEXT,         
  number_floor TEXT,            
  way_in TEXT,                  
  project_name TEXT,            
  project_status TEXT,          
  project_investor TEXT,        
  post_id TEXT,                 
  post_start_time TEXT,         
  post_end_time TEXT,           
  post_type TEXT,               
  source TEXT,                  
  crawled_at TEXT,              
  created_at TIMESTAMP DEFAULT NOW()
);

-- Tạo bảng log processed files
CREATE TABLE bronze.processed_files_log (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    file_path TEXT NOT NULL,
    file_timestamp TIMESTAMP NOT NULL,
    processed_at TIMESTAMP DEFAULT NOW(),
    record_count INTEGER,
    status VARCHAR(20) NOT NULL, -- 'success' hoặc 'failed'
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Grant permissions cho tables
GRANT ALL ON bronze.staging TO service_role;
GRANT ALL ON bronze.processed_files_log TO service_role;
GRANT USAGE ON SCHEMA bronze TO service_role;

```