import argparse
from datetime import datetime, timedelta

def count_weekend_days(year, month):
    weekend_count = 0
    weekend_dates = []
    first_day = datetime(year, month, 1)
    current_day = first_day
    while current_day.month == month:
        if current_day.weekday() in [5, 6]:  # Saturday or Sunday
            weekend_count += 1
            weekend_dates.append(current_day.strftime("%Y-%m-%d"))
        current_day += timedelta(days=1)
    return weekend_count, weekend_dates

def parse_arguments():
    # Get the current date
    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month

    # Set up argument parser
    parser = argparse.ArgumentParser(description="Count the number of weekend days in a given month and year.")
    parser.add_argument("--year", type=int, default=current_year, help="Year to count weekend days (default: current year)")
    parser.add_argument("--month", type=int, default=current_month, help="Month to count weekend days (default: current month)")

    # Parse arguments
    return parser.parse_args()

def main():
    # Parse arguments
    args = parse_arguments()

    # Count the number of weekend days and get the dates
    weekend_count, weekend_dates = count_weekend_days(args.year, args.month)
    
    print(f"The number of weekend days in {args.month}/{args.year} is: {weekend_count}")
    print("The dates that fall on the weekend are:")
    for date in weekend_dates:
        print(date)

if __name__ == "__main__":
    main()
