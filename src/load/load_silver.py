def load_to_silver(supabase, valid_rows, error_rows, on_conflict: str = "subpage_url"):
    # Cơ chế upsert từ staging sang silver
    supabase.upsert(table="clean_table", data=valid_rows, on_conflict=on_conflict, schema="silver")
    
    # Cơ chế insert vào error_table nếu có lỗi
    if error_rows:
        supabase.create(table="error_table", data=error_rows, schema="silver")
    
    return None