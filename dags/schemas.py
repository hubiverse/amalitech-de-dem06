from pydantic import BaseModel, field_validator, model_validator

class FlightRecord(BaseModel):
    airline: str
    source: str
    source_name: str
    destination: str
    destination_name: str
    departure_time: str
    arrival_time: str
    duration_hrs: float
    stopovers: str
    aircraft_type: str
    travel_class: str
    booking_source: str
    base_fare: float
    tax_and_surcharge: float
    total_fare: float | None = None
    seasonality: str
    days_before_departure: int

    @field_validator('base_fare', 'tax_and_surcharge')
    def must_be_positive(cls, v):
        if v < 0:
            raise ValueError("Fares and taxes must be non-negative")
        return v

    @model_validator(mode='after')
    def calculate_total_fare(self):
        expected_total = self.base_fare + self.tax_and_surcharge
        if self.total_fare != expected_total:
            self.total_fare = expected_total
        return self
