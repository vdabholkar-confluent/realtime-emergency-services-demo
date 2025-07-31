CREATE TABLE situational_awareness (
    incident_id INT,
    incident_type STRING,
    severity STRING,
    incident_lat DOUBLE,
    incident_lng DOUBLE,
    incident_status STRING,  -- reported, responding, active, resolved
    description STRING,
    assigned_personnel_count INT,
    assigned_vehicle_count INT,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (incident_id) NOT ENFORCED
) WITH (
    'changelog.mode' = 'append'
);


INSERT INTO situational_awareness
SELECT
    incident_id,
    incident_type,
    severity,
    latitude AS incident_lat,
    longitude AS incident_lng,
    'reported' AS incident_status,
    description,
    0 AS assigned_personnel_count,  -- Will be updated by assignment stream
    0 AS assigned_vehicle_count,    -- Will be updated by assignment stream
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM incidents_raw;

====================================================================================================

CREATE TABLE personnel_risk_status (
    personnel_id INT,
    name STRING,
    department STRING,
    `rank` STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    location_status STRING,  -- available, responding, on_scene, returning
    heart_rate INT,
    oxygen_saturation INT,
    body_temperature DECIMAL(4, 1),
    risk_level STRING,  -- NORMAL, WARNING, CRITICAL
    home_station STRING,
    assigned_incident INT,
    distance_to_incident DECIMAL(6, 2),  -- Distance using UDF
    movement_progress DECIMAL(3, 2),     -- 0.0 to 1.0 for movement simulation
    PRIMARY KEY (personnel_id) NOT ENFORCED
) WITH (
    'changelog.mode' = 'upsert'
);

INSERT INTO personnel_risk_status
SELECT
    p.personnel_id,
    p.name,
    p.department,
    p.`rank`,
    -- Current position (will be updated during movement)
    p.latitude,
    p.longitude,
    -- Status based on assignment
    CASE 
        WHEN nearest_incident.incident_id IS NOT NULL THEN 'responding'
        ELSE 'available'
    END AS location_status,
    -- Simulate realistic vitals based on activity
    CASE 
        WHEN nearest_incident.incident_id IS NOT NULL THEN 85 + (p.personnel_id % 35)  -- Stress response
        ELSE 65 + (p.personnel_id % 20)  -- Resting rate
    END AS heart_rate,
    CASE
        WHEN nearest_incident.incident_id IS NOT NULL THEN 94 + (p.personnel_id % 6)   -- Reduced under stress
        ELSE 98 + (p.personnel_id % 3)   -- Normal range
    END AS oxygen_saturation,
    CASE
        WHEN nearest_incident.incident_id IS NOT NULL THEN CAST(37.5 + ((p.personnel_id % 20) * 0.1) AS DECIMAL(4, 1))
        ELSE CAST(36.4 + ((p.personnel_id % 8) * 0.1) AS DECIMAL(4, 1))
    END AS body_temperature,
    -- Risk assessment
    CASE
        WHEN nearest_incident.incident_id IS NOT NULL AND (85 + (p.personnel_id % 35)) > 140 THEN 'CRITICAL'
        WHEN nearest_incident.incident_id IS NOT NULL AND (85 + (p.personnel_id % 35)) > 110 THEN 'WARNING'
        ELSE 'NORMAL'
    END AS risk_level,
    p.home_station,
    nearest_incident.incident_id AS assigned_incident,
    COALESCE(nearest_incident.distance_km, 0.0) AS distance_to_incident,
    0.0 AS movement_progress  -- Start of journey
FROM personnel_raw p
LEFT JOIN (
    -- Find 3 nearest personnel per incident using UDF
    SELECT 
        incident_id, 
        personnel_id, 
        distance_km
    FROM (
        SELECT 
            i.incident_id,
            p2.personnel_id,
            haversine(p2.latitude, p2.longitude, i.latitude, i.longitude) AS distance_km,
            ROW_NUMBER() OVER (
                PARTITION BY i.incident_id 
                ORDER BY haversine(p2.latitude, p2.longitude, i.latitude, i.longitude)
            ) AS rn
        FROM incidents_raw i
        CROSS JOIN personnel_raw p2
        WHERE p2.department = 'Fire Department'  -- Primary responders
    ) ranked
    WHERE rn <= 3  -- Assign 3 nearest personnel per incident
) nearest_incident ON p.personnel_id = nearest_incident.personnel_id;



====================================================================================================

CREATE TABLE resource_deployment (
    vehicle_id INT,
    call_sign STRING,
    vehicle_type STRING,
    department STRING,
    lat_grid DOUBLE,
    lng_grid DOUBLE,
    status STRING,  -- available, responding, on_scene, returning
    home_station STRING,
    crew_capacity INT,
    assigned_incident INT,
    distance_to_incident DECIMAL(6, 2),  -- Distance using UDF
    movement_progress DECIMAL(3, 2),     -- 0.0 to 1.0 for movement simulation
    total_vehicles INT,
    available_vehicles INT,
    enroute_vehicles INT,
    deployed_vehicles INT,
    PRIMARY KEY (vehicle_id) NOT ENFORCED
) WITH (
    'changelog.mode' = 'upsert'
);

INSERT INTO resource_deployment
SELECT
    v.vehicle_id,
    v.call_sign,
    v.vehicle_type,
    v.department,
    -- Current position (will be updated during movement)
    v.latitude AS lat_grid,
    v.longitude AS lng_grid,
    -- Status based on assignment
    CASE 
        WHEN nearest_incident.incident_id IS NOT NULL THEN 'responding'
        ELSE 'available'
    END AS status,
    v.home_station,
    v.crew_capacity,
    nearest_incident.incident_id AS assigned_incident,
    COALESCE(nearest_incident.distance_km, 0.0) AS distance_to_incident,
    0.0 AS movement_progress,  -- Start of journey
    1 AS total_vehicles,
    CASE WHEN nearest_incident.incident_id IS NULL THEN 1 ELSE 0 END AS available_vehicles,
    CASE WHEN nearest_incident.incident_id IS NOT NULL THEN 1 ELSE 0 END AS enroute_vehicles,
    0 AS deployed_vehicles
FROM vehicles_raw v
LEFT JOIN (
    -- Find 2 nearest vehicles per incident using UDF
    SELECT 
        incident_id, 
        vehicle_id, 
        distance_km
    FROM (
        SELECT 
            i.incident_id,
            v2.vehicle_id,
            haversine(v2.latitude, v2.longitude, i.latitude, i.longitude) AS distance_km,
            ROW_NUMBER() OVER (
                PARTITION BY i.incident_id 
                ORDER BY haversine(v2.latitude, v2.longitude, i.latitude, i.longitude)
            ) AS rn
        FROM incidents_raw i
        CROSS JOIN vehicles_raw v2
    ) ranked
    WHERE rn <= 2  -- Assign 2 nearest vehicles per incident
) nearest_incident ON v.vehicle_id = nearest_incident.vehicle_id;

====================================================================================================

CREATE TABLE active_routes_personnel (
    route_id INT,
    personnel_id INT,
    current_lat DOUBLE,        -- Updated positions during movement
    current_lng DOUBLE,        -- Updated positions during movement
    destination_lat DOUBLE,
    destination_lng DOUBLE,
    incident_id INT,
    incident_type STRING,
    severity STRING,
    distance_km DECIMAL(6, 2),
    eta_minutes INT,
    personnel_status STRING,   -- responding, arrived
    progress_percent DECIMAL(5, 2),  -- Movement progress 0-100%
    PRIMARY KEY (route_id) NOT ENFORCED
) WITH (
    'changelog.mode' = 'upsert'
);


INSERT INTO active_routes_personnel
SELECT
    (p.personnel_id * 1000 + i.incident_id) AS route_id,
    p.personnel_id,
    p.latitude AS current_lat,
    p.longitude AS current_lng,
    i.latitude AS destination_lat,
    i.longitude AS destination_lng,
    i.incident_id,
    i.incident_type,
    i.severity,
    CAST(haversine(p.latitude, p.longitude, i.latitude, i.longitude) AS DECIMAL(6, 2)) AS distance_km,
    -- ETA calculation: distance / speed * 60 (60 km/h for personnel)
    CAST(haversine(p.latitude, p.longitude, i.latitude, i.longitude) / 60 * 60 AS INT) AS eta_minutes,
    'responding' AS personnel_status,
    0.0 AS progress_percent  -- Start of journey
FROM personnel_raw p
JOIN incidents_raw i ON TRUE
WHERE EXISTS (
    -- Only create routes for assigned personnel (using same UDF logic)
    SELECT 1 FROM (
        SELECT 
            i2.incident_id,
            p2.personnel_id,
            ROW_NUMBER() OVER (
                PARTITION BY i2.incident_id 
                ORDER BY haversine(p2.latitude, p2.longitude, i2.latitude, i2.longitude)
            ) AS rn
        FROM incidents_raw i2
        CROSS JOIN personnel_raw p2
        WHERE p2.department = 'Fire Department'
    ) assigned
    WHERE assigned.incident_id = i.incident_id 
    AND assigned.personnel_id = p.personnel_id 
    AND assigned.rn <= 3
);

====================================================================================================

CREATE TABLE active_routes_vehicle (
    route_id INT,
    vehicle_id INT,
    current_lat DOUBLE,        -- Updated positions during movement
    current_lng DOUBLE,        -- Updated positions during movement
    destination_lat DOUBLE,
    destination_lng DOUBLE,
    incident_id INT,
    incident_type STRING,
    severity STRING,
    distance_km DECIMAL(6, 2),
    eta_minutes INT,
    personnel_status STRING,   -- responding, arrived (keeping same name for dashboard compatibility)
    progress_percent DECIMAL(5, 2),  -- Movement progress 0-100%
    PRIMARY KEY (route_id) NOT ENFORCED
) WITH (
    'changelog.mode' = 'upsert'
);


INSERT INTO active_routes_vehicle
SELECT
    (v.vehicle_id * 2000 + i.incident_id) AS route_id,
    v.vehicle_id,
    v.latitude AS current_lat,
    v.longitude AS current_lng,
    i.latitude AS destination_lat,
    i.longitude AS destination_lng,
    i.incident_id,
    i.incident_type,
    i.severity,
    CAST(haversine(v.latitude, v.longitude, i.latitude, i.longitude) AS DECIMAL(6, 2)) AS distance_km,
    -- ETA calculation: distance / speed * 60 (80 km/h for vehicles)
    CAST(haversine(v.latitude, v.longitude, i.latitude, i.longitude) / 80 * 60 AS INT) AS eta_minutes,
    'responding' AS personnel_status,  -- Keeping same field name for dashboard compatibility
    0.0 AS progress_percent  -- Start of journey
FROM vehicles_raw v
JOIN incidents_raw i ON TRUE
WHERE EXISTS (
    -- Only create routes for assigned vehicles (using same UDF logic)
    SELECT 1 FROM (
        SELECT 
            i2.incident_id,
            v2.vehicle_id,
            ROW_NUMBER() OVER (
                PARTITION BY i2.incident_id 
                ORDER BY haversine(v2.latitude, v2.longitude, i2.latitude, i2.longitude)
            ) AS rn
        FROM incidents_raw i2
        CROSS JOIN vehicles_raw v2
    ) assigned
    WHERE assigned.incident_id = i.incident_id 
    AND assigned.vehicle_id = v.vehicle_id 
    AND assigned.rn <= 2
);


