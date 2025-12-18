import polars as pl
from datetime import date
import sys

def process_bookings(input_path, airport_path, start_date, end_date):
    columns = [
        'Airport ID', 'Name', 'City', 'Country', 'IATA', 'ICAO', 
        'Latitude', 'Longitude', 'Altitude', 'Timezone', 'DST', 
        'Tz Database time zone', 'Type', 'Source'
    ]
    
    airports_df = (
        pl.read_csv(
            airport_path, 
            has_header=False, 
            new_columns=columns,
            infer_schema_length=0,
            quote_char='"',
            truncate_ragged_lines=True
        )
        .select(["IATA", "Country"])
        .with_columns([
            pl.col("IATA").replace("\\N", None).str.strip_chars(),
            pl.col("Country").replace("\\N", None)
        ])
        .filter(pl.col("IATA").is_not_null())
        .lazy()
    )

    df = pl.scan_ndjson(input_path)
    
    df = df.select([
        pl.col("event").struct.field("DataElement").struct.field("travelrecord").alias("tr")
    ]).with_columns([
        pl.col("tr").struct.field("passengersList").alias("passengers"),
        pl.col("tr").struct.field("productsList").alias("products")
    ])

    df = df.explode("products").explode("passengers")
    
    df = df.with_columns([
        pl.col("products").struct.field("bookingStatus").alias("status"),
        pl.col("products").struct.field("flight").alias("flight"),
        pl.col("passengers").struct.field("uci").alias("p_uci"),
    ])

    df = df.filter(
        (pl.col("status") == "CONFIRMED") &
        (pl.col("flight").struct.field("marketingAirline") == "KL") &
        (pl.col("flight").struct.field("originAirport").is_in(["AMS", "RTM", "EIN"]))
    )
    df = df.with_columns([
        pl.col("flight").struct.field("departureDate")
            .str.replace("Z", "") 
            .str.replace("T", " ") 
            .str.to_datetime()
            .dt.date()
            .alias("d_date"),
        pl.col("flight").struct.field("destinationAirport").alias("dest")
    ])

    df = df.filter(pl.col("d_date").is_between(start_date, end_date))

    df = df.with_columns([
        pl.col("d_date").dt.weekday().alias("DayOfWeek"),
        pl.col("d_date").dt.month().alias("m"),
    ]).with_columns(
        pl.when(pl.col("m").is_in([3, 4, 5])).then(pl.lit("spring"))
        .when(pl.col("m").is_in([6, 7, 8])).then(pl.lit("summer"))
        .when(pl.col("m").is_in([9, 10, 11])).then(pl.lit("autumn"))
        .otherwise(pl.lit("wintr")).alias("Season")
    )

    final = (
        df.join(airports_df, left_on="dest", right_on="IATA", how="left")
        .group_by(["Country", "DayOfWeek", "Season"])
        .agg(pl.col("p_uci").n_unique().alias("passenger_count"))
        .sort(["Season", "DayOfWeek", "passenger_count"], descending=[False, False, True])
    )

    return final.collect()

if __name__ == "__main__":
    
    BOOKINGS = "./data/bookings/booking.json"
    AIRPORTS = "./data/airports/airports.dat"

    # --- Quick Pre-scan to show user the available date range ---
    print("Scanning dataset for available dates...")
    date_info = (
        pl.scan_ndjson(BOOKINGS)
        .select(
            pl.col("event").struct.field("DataElement")
            .struct.field("travelrecord")
            .struct.field("productsList")
        )
        .explode("productsList")
        .select(
            pl.col("productsList").struct.field("flight")
            .struct.field("departureDate")
            .str.slice(0, 10)
            .str.to_date()
        )
        .select([
            pl.min("departureDate").alias("min_d"),
            pl.max("departureDate").alias("max_d")
        ])
        .collect()
    )
    print(f"Min date: {date_info[0, 'min_d']} Max date: {date_info[0, 'max_d']}")

    print("\nEnter Start Date:")
    sy = int(input("  Year (YYYY): "))
    sm = int(input("  Month (MM): "))
    sd = int(input("  Day (DD): "))
        
    print("\nEnter End Date:")
    ey = int(input("  Year (YYYY): "))
    em = int(input("  Month (MM): "))
    ed = int(input("  Day (DD): "))
        
    start_date = date(sy, sm, sd)
    end_date = date(ey, em, ed)
        
    result = process_bookings(BOOKINGS, AIRPORTS, start_date, end_date)
    
    result.write_csv("result.csv")
    print("Done!")