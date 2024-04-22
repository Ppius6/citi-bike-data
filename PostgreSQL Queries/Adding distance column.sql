ALTER TABLE trips
ADD COLUMN distance_km DOUBLE PRECISION;

UPDATE trips
SET distance_km = 6371 * ACOS (
	LEAST(GREATEST(
		SIN(RADIANS(start_lat)) * SIN(RADIANS(end_lat)) +
		COS(RADIANS(start_lat)) * COS(RADIANS(end_lat)) *
		COS(RADIANS(start_lng) - RADIANS(end_lng))
	, -1), 1)
);

SELECT * FROM trips