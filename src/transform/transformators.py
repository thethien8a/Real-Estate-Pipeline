import pandas as pd

class Transformators:
    def __init__(self):
        pass
    
    @staticmethod
    def clean_price(price):
        return price
    
    @staticmethod
    def clean_area(self, area):
        return area
    
    @staticmethod
    def clean_house_direction(self, house_direction):
        return house_direction
    
    @staticmethod
    def clean_balcony_direction(self, balcony_direction):
        return balcony_direction
    
    @staticmethod
    ...
    
    def transform_row(self, row):
        Transformators.clean_price(row['price'])
        Transformators.clean_area(row['area'])
        Transformators.clean_house_direction(row['house_direction'])
        Transformators.clean_balcony_direction(row['balcony_direction'])
        Transformators.clean_facade(row['facade'])
        Transformators.clean_legal(row['legal'])
        Transformators.clean_furniture(row['furniture'])
        Transformators.clean_number_bedroom(row['number_bedroom'])
        Transformators.clean_number_bathroom(row['number_bathroom'])
        return row

def update_last_processed(last_processed_date):
    return last_processed_date
