BEGIN TRANSACTION;
DROP TABLE IF EXISTS "geometry_columns";
CREATE TABLE geometry_columns (
f_table_name VARCHAR(256) NOT NULL,
f_geometry_column VARCHAR(256) NOT NULL,
type VARCHAR(30) NOT NULL,
coord_dimension INTEGER NOT NULL,
srid INTEGER,
spatial_index_enabled INTEGER NOT NULL);
INSERT INTO "geometry_columns" VALUES('precipitation_radar','geom','POLYGON',2,4326,0);
COMMIT;
