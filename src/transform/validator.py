def validate(row):
    required_fields = ["title", "address", "area", "price", "post_id"]
    return all(row.get(field) for field in required_fields)