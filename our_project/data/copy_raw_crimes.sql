\timing on

COPY raw_staging.raw_crimes (
    id,
    case_number,
    date,
    block,
    iucr,
    primary_type,
    description,
    location_description,
    arrest,
    domestic,
    beat,
    district,
    ward,
    community_area,
    fbi_code,
    x_coordinate,
    y_coordinate,
    year,
    updated_on,
    latitude,
    longitude,
    location
)
FROM '/home/data/raw/Crimes_-_2001_to_Present_20261015.csv'
WITH (
    FORMAT csv,
    HEADER true,
    ENCODING 'utf-8',
    QUOTE '"'
);
