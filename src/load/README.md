Nếu bạn muốn sử dụng project này, bạn cần làm những công việc sau:

1. Vào Supabase, chọn SQL Editor, sau đó chạy các lệnh sau:

CREATE TABLE bronze.staging (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
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
  url TEXT,
  crawled_at TEXT,  
  created_at TIMESTAMP DEFAULT NOW()
);

