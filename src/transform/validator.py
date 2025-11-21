def validate(row):
    required_fields = ["title", "address", "area", "price", "post_id", "post_start_time", "post_end_time", "post_type"]
    return all(row.get(field) for field in required_fields)