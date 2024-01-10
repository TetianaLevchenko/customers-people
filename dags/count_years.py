from datetime import datetime, date

def years_until(target_date):
    
    if isinstance(target_date, date):
        current_date = datetime.now()

        years = current_date.year - target_date.year
        if (current_date.month, current_date.day) < (target_date.month, target_date.day):
            years -= 1
        return years
    else:
        raise ValueError("Input must be a datetime.date object")
   

