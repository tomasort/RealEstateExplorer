-- ALTER TABLE properties ADD COLUMN geog_point geography(POINT, 4326);

UPDATE properties SET geog_point = ST_SetSRID( ST_MakePoint(longitude, latitude), 4326)::geography;

DROP INDEX property_pts_idx;

CREATE INDEX property_pts_idx ON properties USING GIST (geog_point);
