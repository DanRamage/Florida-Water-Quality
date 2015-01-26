BEGIN TRANSACTION;
DROP TABLE IF EXISTS "spatial_ref_sys";
CREATE TABLE spatial_ref_sys (
srid INTEGER NOT NULL PRIMARY KEY,
auth_name VARCHAR(256) NOT NULL,
auth_srid INTEGER NOT NULL,
ref_sys_name VARCHAR(256),
proj4text VARCHAR(2048) NOT NULL);
INSERT INTO "spatial_ref_sys" VALUES(4326,'epsg',4326,'WGS 84','+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs');
COMMIT;
