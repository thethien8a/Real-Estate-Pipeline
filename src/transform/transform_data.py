import pandas as pd
from ..load.supabase_class import SupabaseManager

class TransformData:
    def __init__(self):
        self.supabase = SupabaseManager(default_schema="silver")
        