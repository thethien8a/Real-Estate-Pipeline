import os
from supabase import create_client, Client

# Khởi tạo client
URL_SUPABASE = os.environ.get("SUPABASE_URL")
KEY_SUPABASE = os.environ.get("SUPABASE_KEY")

supabase: Client = create_client(URL_SUPABASE, KEY_SUPABASE)

# Tạo (Create)
def create_user(name, email):
    data = supabase.table("users").insert({"name": name, "email": email}).execute()
    return data

# Đọc (Read)
def get_all_users():
    data = supabase.table("users").select("*").execute()
    return data

# Cập nhật (Update)
def update_user(user_id, name, email):
    data = supabase.table("users").update({"name": name, "email": email}).eq("id", user_id).execute()
    return data

# Xóa (Delete)
def delete_user(user_id):
    data = supabase.table("users").delete().eq("id", user_id).execute()
    return data

# Sử dụng các hàm
new_user = create_user("Nguyễn Văn A", "nguyenvana@example.com")
all_users = get_all_users()
updated_user = update_user(1, "Nguyễn Văn B", "nguyenvanb@example.com")
deleted_user = delete_user(1)
