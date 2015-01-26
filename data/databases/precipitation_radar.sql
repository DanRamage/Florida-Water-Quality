BEGIN TRANSACTION;
DROP TABLE IF EXISTS "precipitation_radar";
CREATE TABLE "precipitation_radar" ("ogc_fid" INTEGER PRIMARY KEY  NOT NULL ,"insert_date" DATETIME NOT NULL ,"collection_date" DATETIME NOT NULL ,"latitude" DOUBLE NOT NULL ,"longitude" DOUBLE NOT NULL ,"precipitation" FLOAT NOT NULL, "geom" POLYGON);
COMMIT;
