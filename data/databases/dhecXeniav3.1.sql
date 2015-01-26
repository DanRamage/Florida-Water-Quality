-- xenia sqlite schema version 3.1 (June 10, 2009)

-- -----------------------------------------------------------------------------------

-- data dictionary section for observations collected

CREATE TABLE obs_type (
    row_id integer PRIMARY KEY,
    standard_name varchar(50),
    definition varchar(1000)
);

CREATE TABLE uom_type (
    row_id integer PRIMARY KEY,
    standard_name varchar(50),
    definition varchar(1000),
    display varchar(50)
);

-- m_scalar_type defines a scalar reference

CREATE TABLE m_scalar_type (
    row_id integer PRIMARY KEY,    
    obs_type_id integer,
    uom_type_id integer
);

-- m_type defines a vector reference (all measurement types are vectors of 1 or more(num_types) scalars)

CREATE TABLE m_type (
    row_id integer PRIMARY KEY,
    num_types integer NOT NULL default 1,
    description varchar(1000),
    m_scalar_type_id integer,
    m_scalar_type_id_2 integer,
    m_scalar_type_id_3 integer,
    m_scalar_type_id_4 integer,
    m_scalar_type_id_5 integer,
    m_scalar_type_id_6 integer,
    m_scalar_type_id_7 integer,
    m_scalar_type_id_8 integer    
);

-- table m_type_display_order is used to determine the order in which measurement types are listed for various displays

CREATE TABLE m_type_display_order (
    row_id integer PRIMARY KEY,
    m_type_id integer NOT NULL
);

-- -----------------------------------------------------------------------------------

-- product_type and timestamp_lkp are used in conjunction with imagery maps and can be ignored if not utilizing this type of metadata for applications


CREATE TABLE product_type (
    row_id integer PRIMARY KEY,
    type_name varchar(50) NOT NULL,
    description varchar(1000)
);

CREATE TABLE timestamp_lkp (
    row_id integer PRIMARY KEY,
    row_entry_date timestamp,
    row_update_date timestamp,
    product_id integer NOT NULL,
    pass_timestamp timestamp,
    filepath varchar(200)
);

-- -----------------------------------------------------------------------------------

-- app_catalog, project are available for use, but not required/utilized at this time
-- normally start with organization table

CREATE TABLE app_catalog (
    row_id integer PRIMARY KEY,
    row_entry_date timestamp default '2008-01-01',
    row_update_date timestamp default '2008-01-01',
    short_name varchar(50) NOT NULL,
    long_name varchar(200),
    description varchar(1000)
);

CREATE TABLE project (
    row_id integer PRIMARY KEY,
    row_entry_date timestamp,
    row_update_date timestamp,
    short_name varchar(50) NOT NULL,
    long_name varchar(200),
    description varchar(1000)
);

-- -----------------------------------------------------------------------------------

-- for table 'metadata' usage see http://code.google.com/p/xenia/wiki/XeniaUpdates section 'sqlite version 3'
-- table metadata not utilized at this time

CREATE TABLE metadata (
    row_id integer PRIMARY KEY,
    row_entry_date timestamp,
    row_update_date timestamp,
    metadata_id integer,
    active integer,
    schema_version varchar(200),
    schema_url varchar(500),
    file_url varchar(500),
    local_filepath varchar(500),
    begin_date timestamp,
    end_date timestamp
);

-- for table custom_fields usage see 'custom_fields' on page XeniaUpdates
-- table custom_fields not utilized at this time

CREATE TABLE custom_fields (
    row_id integer PRIMARY KEY,
    row_entry_date timestamp,
    row_update_date timestamp,
    metadata_id integer,
    ref_table varchar(50),
    ref_row_id integer,
    ref_date timestamp,
    custom_value double precision,
    custom_string varchar(200)    
);


-- -----------------------------------------------------------------------------------


CREATE TABLE organization (
    row_id integer PRIMARY KEY,
    row_entry_date timestamp,
    row_update_date timestamp,
    short_name varchar(50) NOT NULL,
    active integer,
    long_name varchar(200),
    description varchar(1000),
    url varchar(200),
    opendap_url varchar(200)
);

-- platform_type is available for use, but not required/utilized at this time

CREATE TABLE platform_type (
    row_id integer PRIMARY KEY,
    type_name varchar(50) NOT NULL,
    description varchar(1000)
);

CREATE TABLE platform (
    row_id integer PRIMARY KEY,
    row_entry_date timestamp,
    row_update_date timestamp,
    organization_id integer NOT NULL,
    type_id integer,
    short_name varchar(50),
    platform_handle varchar(100) NOT NULL,
    fixed_longitude double precision,
    fixed_latitude double precision,
    active integer,
    begin_date timestamp,
    end_date timestamp,
    project_id integer,
    app_catalog_id integer,
    long_name varchar(200),
    description varchar(1000),
    url varchar(200),
    metadata_id integer    
);

-- sensor_type is available for use, but not required/utilized at this time

CREATE TABLE sensor_type (
    row_id integer PRIMARY KEY,
    type_name varchar(50) NOT NULL,
    description varchar(1000)
);

-- for table 'sensor' usage
-- only the NOT NULL fields in the sensor table are required to be populated, the other fields are included for metadata tracking purposes
-- report_interval default unit is minutes
-- see also http://code.google.com/p/xenia/wiki/InstrumentationExamples


CREATE TABLE sensor (
    row_id integer PRIMARY KEY,
    row_entry_date timestamp,
    row_update_date timestamp,
    platform_id integer NOT NULL,
    type_id integer,
    short_name varchar(50),
    m_type_id integer NOT NULL,
    fixed_z double precision,
    active integer,
    begin_date timestamp,
    end_date timestamp,
    s_order integer NOT NULL default 1,
    url varchar(200),
    metadata_id integer,
    report_interval integer
);

CREATE TABLE multi_obs (
    row_id integer PRIMARY KEY,
    row_entry_date timestamp,
    row_update_date timestamp,
    platform_handle varchar(100) NOT NULL,
    sensor_id integer NOT NULL,
    m_type_id integer NOT NULL,
    m_date timestamp NOT NULL,
    m_lon double precision,
    m_lat double precision,
    m_z double precision,
    m_value double precision,
    m_value_2 double precision,
    m_value_3 double precision, 
    m_value_4 double precision, 
    m_value_5 double precision, 
    m_value_6 double precision, 
    m_value_7 double precision, 
    m_value_8 double precision,     
    qc_metadata_id integer,
    qc_level integer,
    qc_flag varchar(100),
    qc_metadata_id_2 integer,
    qc_level_2 integer,
    qc_flag_2 varchar(100),
    metadata_id integer,
    d_label_theta integer,
    d_top_of_hour integer,
    d_report_hour timestamp
);

-- -----------------------------------------------------------------------------------

CREATE UNIQUE INDEX i_platform ON platform (platform_handle);

CREATE UNIQUE INDEX i_sensor ON sensor (platform_id,m_type_id,s_order);

CREATE UNIQUE INDEX i_multi_obs ON multi_obs (m_date, m_type_id, sensor_id);




DROP TABLE IF EXISTS "obs_type";
CREATE TABLE obs_type (
    row_id integer PRIMARY KEY,
    standard_name varchar(50),
    definition varchar(1000)
);
INSERT INTO "obs_type" VALUES(1,'wind_speed','Wind speed');
INSERT INTO "obs_type" VALUES(2,'wind_gust','Maximum instantaneous wind speed (usually no more than but not limited to 10 seconds) within a sample averaging interval');
INSERT INTO "obs_type" VALUES(3,'wind_from_direction','Direction from which wind is blowing.  Meteorological Convention.');
INSERT INTO "obs_type" VALUES(4,'air_pressure','Pressure exerted by overlying air.');
INSERT INTO "obs_type" VALUES(5,'air_temperature','Temperature of air, in situ.');
INSERT INTO "obs_type" VALUES(6,'water_temperature','Water temperature');
INSERT INTO "obs_type" VALUES(7,'water_conductivity','Ability of a specific volume (1 cubic centimeter) of water to pass an electrical current');
INSERT INTO "obs_type" VALUES(8,'water_pressure','Water Pressure');
INSERT INTO "obs_type" VALUES(9,'water_salinity','Water Salinity');
INSERT INTO "obs_type" VALUES(10,'chl_concentration','concentration of cholorophyll-a in a defined volume of water');
INSERT INTO "obs_type" VALUES(11,'current_speed','Water Current Magnitude');
INSERT INTO "obs_type" VALUES(12,'current_to_direction','Direction toward which current is flowing');
INSERT INTO "obs_type" VALUES(13,'significant_wave_height','Significant wave height');
INSERT INTO "obs_type" VALUES(14,'dominant_wave_period','Dominant Wave period');
INSERT INTO "obs_type" VALUES(15,'significant_wave_to_direction','Significant Wave Direction');
INSERT INTO "obs_type" VALUES(18,'sea_surface_temperature','Surface Water temperature');
INSERT INTO "obs_type" VALUES(19,'sea_bottom_temperature','Bottom Water temperature');
INSERT INTO "obs_type" VALUES(20,'sea_surface_eastward_current','East/West component of ocean current near the sea surface, Eastward is positive');
INSERT INTO "obs_type" VALUES(21,'sea_surface_northward_current','North/South component of ocean current near the sea surface, Northward is positive');
INSERT INTO "obs_type" VALUES(22,'relative_humidity','Relative humidity');
INSERT INTO "obs_type" VALUES(23,'water_level','water_level');
INSERT INTO "obs_type" VALUES(24,'bottom_water_salinity','Bottom Water Salinity');
INSERT INTO "obs_type" VALUES(25,'surface_water_salinity','Surface Water Salinity');
INSERT INTO "obs_type" VALUES(26,'bottom_chlorophyll','Bottom Water Chlorophyll');
INSERT INTO "obs_type" VALUES(27,'surface_chlorophyll','Bottom Water Chlorophyll');
INSERT INTO "obs_type" VALUES(28,'salinity','salinity');
INSERT INTO "obs_type" VALUES(29,'precipitation','measured precipitation or rainfall');
INSERT INTO "obs_type" VALUES(30,'solar_radiation','measured solar radiation or sunlight');
INSERT INTO "obs_type" VALUES(31,'eastward_current','East/West component of water current, Eastward is positive');
INSERT INTO "obs_type" VALUES(32,'northward_current','North/South component of water current, Northward is positive');
INSERT INTO "obs_type" VALUES(33,'precipitation_accumulated_daily','measured precipitation or rainfall daily accumulation');
INSERT INTO "obs_type" VALUES(34,'oxygen_concentration','concentration of oxygen in a defined volume of water');
INSERT INTO "obs_type" VALUES(35,'turbidity','Measure of light scattering due to suspended material in water.');
INSERT INTO "obs_type" VALUES(36,'ph','(from potential of Hydrogen) the logarithm of the reciprocal of hydrogen-ion concentration in gram atoms per liter');
INSERT INTO "obs_type" VALUES(37,'visibility','Greatest distance an object can be seen and identified. Usually refering to visibility in air');
INSERT INTO "obs_type" VALUES(38,'precipitation_accumulated_storm','measured precipitation or rainfall storm accumulation');
INSERT INTO "obs_type" VALUES(39,'drifter_speed','drifter speed');
INSERT INTO "obs_type" VALUES(40,'drifter_direction','direction which drifter is moving in degrees from North');
INSERT INTO "obs_type" VALUES(41,'gage_height','gage_height');
INSERT INTO "obs_type" VALUES(42,'depth','approximate water depth');
INSERT INTO "obs_type" VALUES(43,'discharge','cubic volume discharge');
INSERT INTO "obs_type" VALUES(44,'stream_velocity','inland stream velocity');
INSERT INTO "obs_type" VALUES(45,'mean_wave_direction_peak_period','NDBC definition - Mean wave direction corresponding to energy of the dominant period (DPD). The units are degrees from true North just like wind direction.');
INSERT INTO "obs_type" VALUES(46,'average_wave_period','Average period (seconds) of the highest one-third of the wave observed during 1 20 minute sampling period');
INSERT INTO "obs_type" VALUES(47,'swell_height','vertical distance (meters) between any swell crest and the succeeding swell wave trough');
INSERT INTO "obs_type" VALUES(48,'swell_period','the time (usually measured in seconds) that takes successive swell wave crests or troughs pass a fixed point');
INSERT INTO "obs_type" VALUES(49,'swell_direction','the compass direction from which the swell wave are coming from');
INSERT INTO "obs_type" VALUES(50,'wind_wave_height','vertical distance (meters) between any wind wave crest and the succeeding wind wave trough');
INSERT INTO "obs_type" VALUES(51,'wind_wave_period','the time (in seconds) that it takes successive wind wave crests or troughs to pass a fixed point');
INSERT INTO "obs_type" VALUES(52,'wind_wave_direction','the compass direction (in degrees) from which the wind waves are coming');
INSERT INTO "obs_type" VALUES(53,'directional_wave_parameter','');
INSERT INTO "obs_type" VALUES(54,'principal_wave_direction','');
INSERT INTO "obs_type" VALUES(55,'spectral_energy','');
INSERT INTO "obs_type" VALUES(56,'center_frequencies','');
INSERT INTO "obs_type" VALUES(57,'swell_wave_direction','');
INSERT INTO "obs_type" VALUES(58,'bandwidths','');
INSERT INTO "obs_type" VALUES(60,'mean_wave_direction_peak_period','');
INSERT INTO "obs_type" VALUES(61,'polar_coordinate_r_1','');
INSERT INTO "obs_type" VALUES(62,'polar_coordinate_r_2','');
INSERT INTO "obs_type" VALUES(63,'fourier_coefficient_a_1','');
INSERT INTO "obs_type" VALUES(64,'fourier_coefficient_b_1','');
INSERT INTO "obs_type" VALUES(65,'fourier_coefficient_a_2','');
INSERT INTO "obs_type" VALUES(66,'fourier_coefficient_b_2','');
INSERT INTO "obs_type" VALUES(67,'battery_voltage','Battery voltage.');
INSERT INTO "obs_type" VALUES(68,'program_code','The program code used for an instrument/sensor.');

DROP TABLE IF EXISTS "uom_type";
CREATE TABLE uom_type (
    row_id integer PRIMARY KEY,
    standard_name varchar(50),
    definition varchar(1000),
    display varchar(50)
);
INSERT INTO "uom_type" VALUES(1,'m_s-1','meters per second','m/s');
INSERT INTO "uom_type" VALUES(2,'degrees_true','degrees clockwise from true north','deg');
INSERT INTO "uom_type" VALUES(3,'celsius','degrees celsius','deg C');
INSERT INTO "uom_type" VALUES(4,'mb','1 mb = 0.001 bar = 100 Pa = 1 000 dyn/cm^2','mb');
INSERT INTO "uom_type" VALUES(5,'mS_cm-1','milliSiemens per centimeter','mS/cm');
INSERT INTO "uom_type" VALUES(6,'m','meter','m');
INSERT INTO "uom_type" VALUES(7,'s','seconds','s');
INSERT INTO "uom_type" VALUES(8,'cm_s-1','centrimeter per second','cm/s');
INSERT INTO "uom_type" VALUES(9,'millibar','millibar','mb');
INSERT INTO "uom_type" VALUES(10,'psu','practical salinity units','psu');
INSERT INTO "uom_type" VALUES(11,'percent','percentage','%');
INSERT INTO "uom_type" VALUES(12,'ppt','parts per thousand','ppt');
INSERT INTO "uom_type" VALUES(13,'ug_L-1','micrograms per liter','ug/L');
INSERT INTO "uom_type" VALUES(14,'millimeter','millimeter','mm');
INSERT INTO "uom_type" VALUES(15,'millimoles_per_m^2','millimoles per meter squared','millimoles per m^2');
INSERT INTO "uom_type" VALUES(16,'units','a dimensionless unit','units');
INSERT INTO "uom_type" VALUES(17,'ntu','nephelometric turbidity units','ntu');
INSERT INTO "uom_type" VALUES(18,'mg_L-1','milligrams per liter','mg/L');
INSERT INTO "uom_type" VALUES(19,'nautical_miles','equal to 1.151 statue miles','nautical miles');
INSERT INTO "uom_type" VALUES(20,'mph','miles per hour','mph');
INSERT INTO "uom_type" VALUES(21,'knots','knots','knots');
INSERT INTO "uom_type" VALUES(22,'ft','feet','ft');
INSERT INTO "uom_type" VALUES(23,'cubic_ft_s-1','cubic feet per second','ft^3/s');
INSERT INTO "uom_type" VALUES(24,'ft_s-1','feet per second','ft/s');
INSERT INTO "uom_type" VALUES(25,'m^2_Hz-1','square meter per hz','m**2/Hz');
INSERT INTO "uom_type" VALUES(26,'Hz','Hz','Hz');
INSERT INTO "uom_type" VALUES(27,'in','inches','in');
INSERT INTO "uom_type" VALUES(28,'V','voltage','V');

DROP TABLE IF EXISTS "m_type_display_order";
CREATE TABLE m_type_display_order (
    row_id integer PRIMARY KEY,
    m_type_id integer NOT NULL
);
INSERT INTO "m_type_display_order" VALUES(1,5);
INSERT INTO "m_type_display_order" VALUES(2,3);
INSERT INTO "m_type_display_order" VALUES(3,1);
INSERT INTO "m_type_display_order" VALUES(4,2);
INSERT INTO "m_type_display_order" VALUES(5,4);
INSERT INTO "m_type_display_order" VALUES(6,22);
INSERT INTO "m_type_display_order" VALUES(7,39);
INSERT INTO "m_type_display_order" VALUES(8,30);
INSERT INTO "m_type_display_order" VALUES(9,29);
INSERT INTO "m_type_display_order" VALUES(10,33);
INSERT INTO "m_type_display_order" VALUES(11,40);
INSERT INTO "m_type_display_order" VALUES(12,23);
INSERT INTO "m_type_display_order" VALUES(13,8);
INSERT INTO "m_type_display_order" VALUES(14,13);
INSERT INTO "m_type_display_order" VALUES(15,14);
INSERT INTO "m_type_display_order" VALUES(16,52);
INSERT INTO "m_type_display_order" VALUES(17,18);
INSERT INTO "m_type_display_order" VALUES(18,20);
INSERT INTO "m_type_display_order" VALUES(19,21);
INSERT INTO "m_type_display_order" VALUES(20,11);
INSERT INTO "m_type_display_order" VALUES(21,12);
INSERT INTO "m_type_display_order" VALUES(22,31);
INSERT INTO "m_type_display_order" VALUES(23,32);
INSERT INTO "m_type_display_order" VALUES(24,19);
INSERT INTO "m_type_display_order" VALUES(25,6);
INSERT INTO "m_type_display_order" VALUES(26,7);
INSERT INTO "m_type_display_order" VALUES(27,25);
INSERT INTO "m_type_display_order" VALUES(28,24);
INSERT INTO "m_type_display_order" VALUES(29,9);
INSERT INTO "m_type_display_order" VALUES(30,28);
INSERT INTO "m_type_display_order" VALUES(31,27);
INSERT INTO "m_type_display_order" VALUES(32,26);
INSERT INTO "m_type_display_order" VALUES(33,10);
INSERT INTO "m_type_display_order" VALUES(34,34);
INSERT INTO "m_type_display_order" VALUES(35,35);
INSERT INTO "m_type_display_order" VALUES(36,36);
INSERT INTO "m_type_display_order" VALUES(37,38);
INSERT INTO "m_type_display_order" VALUES(38,47);
INSERT INTO "m_type_display_order" VALUES(39,48);
INSERT INTO "m_type_display_order" VALUES(40,49);

DROP TABLE IF EXISTS "m_scalar_type";
CREATE TABLE m_scalar_type (
    row_id integer PRIMARY KEY,    
    obs_type_id integer,
    uom_type_id integer
);
INSERT INTO "m_scalar_type" VALUES(1,1,1);
INSERT INTO "m_scalar_type" VALUES(2,2,1);
INSERT INTO "m_scalar_type" VALUES(3,3,2);
INSERT INTO "m_scalar_type" VALUES(4,4,4);
INSERT INTO "m_scalar_type" VALUES(5,5,3);
INSERT INTO "m_scalar_type" VALUES(6,6,3);
INSERT INTO "m_scalar_type" VALUES(7,7,5);
INSERT INTO "m_scalar_type" VALUES(8,8,4);
INSERT INTO "m_scalar_type" VALUES(9,9,10);
INSERT INTO "m_scalar_type" VALUES(10,10,13);
INSERT INTO "m_scalar_type" VALUES(11,11,8);
INSERT INTO "m_scalar_type" VALUES(12,12,2);
INSERT INTO "m_scalar_type" VALUES(13,13,6);
INSERT INTO "m_scalar_type" VALUES(14,14,7);
INSERT INTO "m_scalar_type" VALUES(15,15,2);
INSERT INTO "m_scalar_type" VALUES(18,18,3);
INSERT INTO "m_scalar_type" VALUES(19,19,3);
INSERT INTO "m_scalar_type" VALUES(20,20,8);
INSERT INTO "m_scalar_type" VALUES(21,21,8);
INSERT INTO "m_scalar_type" VALUES(22,22,11);
INSERT INTO "m_scalar_type" VALUES(23,23,6);
INSERT INTO "m_scalar_type" VALUES(24,24,10);
INSERT INTO "m_scalar_type" VALUES(25,25,10);
INSERT INTO "m_scalar_type" VALUES(26,26,13);
INSERT INTO "m_scalar_type" VALUES(27,27,13);
INSERT INTO "m_scalar_type" VALUES(28,28,10);
INSERT INTO "m_scalar_type" VALUES(29,29,14);
INSERT INTO "m_scalar_type" VALUES(30,30,15);
INSERT INTO "m_scalar_type" VALUES(31,31,8);
INSERT INTO "m_scalar_type" VALUES(32,32,8);
INSERT INTO "m_scalar_type" VALUES(33,33,14);
INSERT INTO "m_scalar_type" VALUES(34,34,18);
INSERT INTO "m_scalar_type" VALUES(35,34,11);
INSERT INTO "m_scalar_type" VALUES(36,35,17);
INSERT INTO "m_scalar_type" VALUES(38,36,16);
INSERT INTO "m_scalar_type" VALUES(39,37,19);
INSERT INTO "m_scalar_type" VALUES(40,38,14);
INSERT INTO "m_scalar_type" VALUES(41,39,1);
INSERT INTO "m_scalar_type" VALUES(42,39,20);
INSERT INTO "m_scalar_type" VALUES(43,39,21);
INSERT INTO "m_scalar_type" VALUES(44,40,2);
INSERT INTO "m_scalar_type" VALUES(45,31,1);
INSERT INTO "m_scalar_type" VALUES(46,32,1);
INSERT INTO "m_scalar_type" VALUES(47,41,6);
INSERT INTO "m_scalar_type" VALUES(48,42,6);
INSERT INTO "m_scalar_type" VALUES(49,43,23);
INSERT INTO "m_scalar_type" VALUES(50,44,24);
INSERT INTO "m_scalar_type" VALUES(51,11,1);
INSERT INTO "m_scalar_type" VALUES(52,45,2);
INSERT INTO "m_scalar_type" VALUES(53,46,7);
INSERT INTO "m_scalar_type" VALUES(54,47,6);
INSERT INTO "m_scalar_type" VALUES(55,48,7);
INSERT INTO "m_scalar_type" VALUES(56,49,2);
INSERT INTO "m_scalar_type" VALUES(57,50,6);
INSERT INTO "m_scalar_type" VALUES(58,51,7);
INSERT INTO "m_scalar_type" VALUES(59,52,2);
INSERT INTO "m_scalar_type" VALUES(60,53,2);
INSERT INTO "m_scalar_type" VALUES(61,54,2);
INSERT INTO "m_scalar_type" VALUES(62,55,25);
INSERT INTO "m_scalar_type" VALUES(63,56,26);
INSERT INTO "m_scalar_type" VALUES(64,57,2);
INSERT INTO "m_scalar_type" VALUES(65,58,26);
INSERT INTO "m_scalar_type" VALUES(66,60,7);
INSERT INTO "m_scalar_type" VALUES(67,29,27);
INSERT INTO "m_scalar_type" VALUES(68,68,20);
INSERT INTO "m_scalar_type" VALUES(69,69,2);
INSERT INTO "m_scalar_type" VALUES(70,67,28);
INSERT INTO "m_scalar_type" VALUES(71,68,16);
INSERT INTO "m_scalar_type" VALUES(72,33,27);
INSERT INTO "m_scalar_type" VALUES(73,1,20);

DROP TABLE IF EXISTS "m_type";
CREATE TABLE m_type (
    row_id integer PRIMARY KEY,
    num_types integer NOT NULL default 1,
    description varchar(1000),
    m_scalar_type_id integer,
    m_scalar_type_id_2 integer,
    m_scalar_type_id_3 integer,
    m_scalar_type_id_4 integer,
    m_scalar_type_id_5 integer,
    m_scalar_type_id_6 integer,
    m_scalar_type_id_7 integer,
    m_scalar_type_id_8 integer    
);
INSERT INTO "m_type" VALUES(1,1,NULL,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(2,1,NULL,2,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(3,1,NULL,3,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(4,1,NULL,4,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(5,1,NULL,5,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(6,1,NULL,6,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(7,1,NULL,7,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(8,1,NULL,8,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(9,1,NULL,9,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(10,1,NULL,10,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(11,1,NULL,11,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(12,1,NULL,12,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(13,1,NULL,13,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(14,1,NULL,14,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(15,1,NULL,15,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(18,1,NULL,18,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(19,1,NULL,19,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(20,1,NULL,20,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(21,1,NULL,21,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(22,1,NULL,22,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(23,1,NULL,23,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(24,1,NULL,24,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(25,1,NULL,25,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(26,1,NULL,26,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(27,1,NULL,27,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(28,1,NULL,28,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(29,1,NULL,29,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(30,1,NULL,30,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(31,1,NULL,31,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(32,1,NULL,32,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(33,1,NULL,33,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(34,1,NULL,34,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(35,1,NULL,35,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(36,1,NULL,36,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(38,1,NULL,38,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(39,1,NULL,39,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(40,1,NULL,40,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(41,4,NULL,41,42,43,44,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(42,2,NULL,45,46,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(43,1,NULL,47,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(44,1,NULL,48,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(45,1,NULL,49,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(46,1,NULL,50,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(47,1,NULL,51,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(48,1,NULL,52,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(49,1,NULL,53,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(51,1,NULL,54,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(52,1,NULL,55,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(53,1,NULL,56,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(54,1,NULL,57,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(55,1,NULL,58,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(56,1,NULL,59,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(57,1,NULL,61,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(58,1,NULL,62,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(59,1,NULL,63,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(60,1,NULL,64,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(61,1,NULL,65,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(62,1,NULL,66,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(63,1,NULL,67,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(64,1,NULL,68,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(65,1,NULL,69,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(66,3,'Type for SCDHEC rain gauges.',67,70,71,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(67,1,NULL,72,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "m_type" VALUES(68,1,NULL,73,NULL,NULL,NULL,NULL,NULL,NULL,NULL);

DROP TABLE IF EXISTS "organization";
CREATE TABLE organization (
    row_id integer PRIMARY KEY,
    row_entry_date timestamp,
    row_update_date timestamp,
    short_name varchar(50) NOT NULL,
    active integer,
    long_name varchar(200),
    description varchar(1000),
    url varchar(200),
    opendap_url varchar(200)
);
INSERT INTO "organization" VALUES(1,NULL,NULL,'dhec',NULL,NULL,'South Carolina Department of Heath and Environmental Control',NULL,NULL);

DROP TABLE IF EXISTS "platform";
CREATE TABLE platform (
    row_id integer PRIMARY KEY,
    row_entry_date timestamp,
    row_update_date timestamp,
    organization_id integer NOT NULL,
    type_id integer,
    short_name varchar(50),
    platform_handle varchar(100) NOT NULL,
    fixed_longitude double precision,
    fixed_latitude double precision,
    active integer,
    begin_date timestamp,
    end_date timestamp,
    project_id integer,
    app_catalog_id integer,
    long_name varchar(200),
    description varchar(1000),
    url varchar(200),
    metadata_id integer    
);
INSERT INTO "platform" VALUES(1,NULL,NULL,1,NULL,'nmb1','dhec.nmb1.raingauge',-78.608666,33.839933,1,NULL,NULL,NULL,NULL,NULL,'59th Avenue N',NULL,NULL);
INSERT INTO "platform" VALUES(2,NULL,NULL,1,NULL,'nmb2','dhec.nmb2.raingauge',-78.667709,33.820429,1,NULL,NULL,NULL,NULL,NULL,'4th Avenue N',NULL,NULL);
INSERT INTO "platform" VALUES(3,NULL,NULL,1,NULL,'nmb3','dhec.nmb3.raingauge',-78.736148,33.79882,1,NULL,NULL,NULL,NULL,NULL,'46th Avenue S',NULL,NULL);
INSERT INTO "platform" VALUES(4,NULL,NULL,1,NULL,'mb1','dhec.mb1.raingauge',-78.782589,33.764152,1,NULL,NULL,NULL,NULL,NULL,'Apache Campground Pier',NULL,NULL);
INSERT INTO "platform" VALUES(5,NULL,NULL,1,NULL,'mb2','dhec.mb2.raingauge',-78.841869,33.723773,1,NULL,NULL,NULL,NULL,NULL,'52nd Avenue N Pump Station',NULL,NULL);
INSERT INTO "platform" VALUES(6,NULL,NULL,1,NULL,'mb3','dhec.mb3.raingauge',-78.892783,33.68146,1,NULL,NULL,NULL,NULL,NULL,'5th Avenue S Pump Station',NULL,NULL);
INSERT INTO "platform" VALUES(7,NULL,NULL,1,NULL,'mb4','dhec.mb4.raingauge',-78.917422,33.671589,1,NULL,NULL,NULL,NULL,NULL,'West of Hwy17, adj. to Airport',NULL,NULL);
INSERT INTO "platform" VALUES(8,NULL,NULL,1,NULL,'surfside','dhec.surfside.raingauge',-78.963161,33.6315,1,NULL,NULL,NULL,NULL,NULL,'Dick Pond Rd at GCWQSA Pump Statio',NULL,NULL);
INSERT INTO "platform" VALUES(9,NULL,NULL,1,NULL,'gardcty','dhec.gardcty.raingauge',-78.986643,33.590552,1,NULL,NULL,NULL,NULL,NULL,'South of Woodland Dr at GCWSA Pump Station',NULL,NULL);
INSERT INTO "platform" VALUES(10,NULL,NULL,1,NULL,'murrelsinlet','dhec.murrelsinlet.raingauge',-79.029565,33.562622,1,NULL,NULL,NULL,NULL,NULL,'',NULL,NULL);
INSERT INTO "platform" VALUES(11,NULL,NULL,1,NULL,'WAC-001','dhec.WAC-001.monitorstation',-78.608124,33.838112,1,NULL,NULL,NULL,NULL,NULL,'59th Ave N',NULL,NULL);
INSERT INTO "platform" VALUES(12,NULL,NULL,1,NULL,'WAC-002','dhec.WAC-002.monitorstation',-78.623384,33.833667,1,NULL,NULL,NULL,NULL,NULL,'45th Ave N',NULL,NULL);
INSERT INTO "platform" VALUES(13,NULL,NULL,1,NULL,'WAC-003','dhec.WAC-003.monitorstation',-78.637238,33.82885,1,NULL,NULL,NULL,NULL,NULL,'30th Ave N',NULL,NULL);
INSERT INTO "platform" VALUES(14,NULL,NULL,1,NULL,'WAC-004','dhec.WAC-004.monitorstation',-78.653389,33.824127,1,NULL,NULL,NULL,NULL,NULL,'16th Ave N',NULL,NULL);
INSERT INTO "platform" VALUES(15,NULL,NULL,1,NULL,'WAC-005','dhec.WAC-005.monitorstation',-78.668182,33.81889,1,NULL,NULL,NULL,NULL,NULL,'3rd Ave N',NULL,NULL);
INSERT INTO "platform" VALUES(16,NULL,NULL,1,NULL,'WAC-005A','dhec.WAC-005A.monitorstation',-78.681816,33.813919,1,NULL,NULL,NULL,NULL,NULL,'7th Ave S',NULL,NULL);
INSERT INTO "platform" VALUES(17,NULL,NULL,1,NULL,'WAC-006','dhec.WAC-006.monitorstation',-78.684113,33.813141,1,NULL,NULL,NULL,NULL,NULL,'9th Ave S',NULL,NULL);
INSERT INTO "platform" VALUES(18,NULL,NULL,1,NULL,'WAC-007','dhec.WAC-007.monitorstation',-78.700081,33.806564,1,NULL,NULL,NULL,NULL,NULL,'17th Ave S',NULL,NULL);
INSERT INTO "platform" VALUES(19,NULL,NULL,1,NULL,'WAC-008','dhec.WAC-008.monitorstation',-78.717644,33.798526,1,NULL,NULL,NULL,NULL,NULL,'33rd Ave S',NULL,NULL);
INSERT INTO "platform" VALUES(20,NULL,NULL,1,NULL,'WAC-009','dhec.WAC-009.monitorstation',-78.731698,33.791639,1,NULL,NULL,NULL,NULL,NULL,'47th Ave. S',NULL,NULL);
INSERT INTO "platform" VALUES(21,NULL,NULL,1,NULL,'WAC-009A','dhec.WAC-009A.monitorstation',-78.73913,33.787536,1,NULL,NULL,NULL,NULL,NULL,'White Point Swash',NULL,NULL);
INSERT INTO "platform" VALUES(22,NULL,NULL,1,NULL,'WAC-010','dhec.WAC-010.monitorstation',-78.741882,33.786366,1,NULL,NULL,NULL,NULL,NULL,'Briarcliff Cabana',NULL,NULL);
INSERT INTO "platform" VALUES(23,NULL,NULL,1,NULL,'WAC-011','dhec.WAC-011.monitorstation',-78.745659,33.784439,1,NULL,NULL,NULL,NULL,NULL,'2m N of Wyndham Hotel',NULL,NULL);
INSERT INTO "platform" VALUES(24,NULL,NULL,1,NULL,'WAC-012','dhec.WAC-012.monitorstation',-78.764427,33.773857,1,NULL,NULL,NULL,NULL,NULL,'Lands End Resort Arcadia',NULL,NULL);
INSERT INTO "platform" VALUES(25,NULL,NULL,1,NULL,'WAC-013','dhec.WAC-013.monitorstation',-78.77519,33.767888,1,NULL,NULL,NULL,NULL,NULL,'Wyhdam Hotel Arcadia',NULL,NULL);
INSERT INTO "platform" VALUES(26,NULL,NULL,1,NULL,'WAC-014','dhec.WAC-014.monitorstation',-78.788451,33.759365,1,NULL,NULL,NULL,NULL,NULL,'Sands Ocean Club Arcadia',NULL,NULL);
INSERT INTO "platform" VALUES(27,NULL,NULL,1,NULL,'WAC-015','dhec.WAC-015.monitorstation',-78.794008,33.755785,1,NULL,NULL,NULL,NULL,NULL,'Singleton Swash Arcadia',NULL,NULL);
INSERT INTO "platform" VALUES(28,NULL,NULL,1,NULL,'WAC-015A','dhec.WAC-015A.monitorstation',-78.803427,33.7498,1,NULL,NULL,NULL,NULL,NULL,'Bear Branch Swash',NULL,NULL);
INSERT INTO "platform" VALUES(29,NULL,NULL,1,NULL,'WAC-016','dhec.WAC-016.monitorstation',-78.812889,33.743202,1,NULL,NULL,NULL,NULL,NULL,'77th Ave North, MB',NULL,NULL);
INSERT INTO "platform" VALUES(30,NULL,NULL,1,NULL,'WAC-016A','dhec.WAC-016A.monitorstation',-78.82225,33.736961,1,NULL,NULL,NULL,NULL,NULL,'Cane Patch Swatch',NULL,NULL);
INSERT INTO "platform" VALUES(31,NULL,NULL,1,NULL,'WAC-017','dhec.WAC-017.monitorstation',-78.82592,33.734222,1,NULL,NULL,NULL,NULL,NULL,'64th Ave North, MB',NULL,NULL);
INSERT INTO "platform" VALUES(32,NULL,NULL,1,NULL,'WAC-017A','dhec.WAC-017A.monitorstation',-78.838081,33.724979,1,NULL,NULL,NULL,NULL,NULL,'Deep Head Swash, MB',NULL,NULL);
INSERT INTO "platform" VALUES(33,NULL,NULL,1,NULL,'WAC-018','dhec.WAC-018.monitorstation',-78.84269,33.721703,1,NULL,NULL,NULL,NULL,NULL,'50th Ave North, MB',NULL,NULL);
INSERT INTO "platform" VALUES(34,NULL,NULL,1,NULL,'WAC-019','dhec.WAC-019.monitorstation',-78.857109,33.710392,1,NULL,NULL,NULL,NULL,NULL,'34th Ave North, MB',NULL,NULL);
INSERT INTO "platform" VALUES(35,NULL,NULL,1,NULL,'WAC-020','dhec.WAC-020.monitorstation',-78.866264,33.702896,1,NULL,NULL,NULL,NULL,NULL,'24th Ave North, MB',NULL,NULL);
INSERT INTO "platform" VALUES(36,NULL,NULL,1,NULL,'WAC-021','dhec.WAC-021.monitorstation',-78.880002,33.690417,1,NULL,NULL,NULL,NULL,NULL,'8th Ave North, MB',NULL,NULL);
INSERT INTO "platform" VALUES(37,NULL,NULL,1,NULL,'WAC-22A','dhec.WAC-22A.monitorstation',-78.890739,33.680092,1,NULL,NULL,NULL,NULL,NULL,'WIthers Swash',NULL,NULL);
INSERT INTO "platform" VALUES(38,NULL,NULL,1,NULL,'WAC-024','dhec.WAC-024.monitorstation',-78.907852,33.666485,1,NULL,NULL,NULL,NULL,NULL,'23rd Ave South, MB',NULL,NULL);
INSERT INTO "platform" VALUES(39,NULL,NULL,1,NULL,'WAC-025A','dhec.WAC-025A.monitorstation',-78.917053,33.658138,1,NULL,NULL,NULL,NULL,NULL,'Midway Swash',NULL,NULL);
INSERT INTO "platform" VALUES(40,NULL,NULL,1,NULL,'WAC-026','dhec.WAC-026.monitorstation',-78.921021,33.654804,1,NULL,NULL,NULL,NULL,NULL,'Nash Drive',NULL,NULL);
INSERT INTO "platform" VALUES(41,NULL,NULL,1,NULL,'WAC-027','dhec.WAC-027.monitorstation',-78.932224,33.645399,1,NULL,NULL,NULL,NULL,NULL,'Myrtle Beach State Park',NULL,NULL);
INSERT INTO "platform" VALUES(42,NULL,NULL,1,NULL,'WAC-028','dhec.WAC-028.monitorstation',-78.944862,33.632813,1,NULL,NULL,NULL,NULL,NULL,'Pirateland Swash',NULL,NULL);
INSERT INTO "platform" VALUES(43,NULL,NULL,1,NULL,'WAC-029','dhec.WAC-029.monitorstation',-78.952248,33.625679,1,NULL,NULL,NULL,NULL,NULL,'Ocean Lakes Campground',NULL,NULL);
INSERT INTO "platform" VALUES(44,NULL,NULL,1,NULL,'WAC-029A','dhec.WAC-029A.monitorstation',-78.958427,33.619007,1,NULL,NULL,NULL,NULL,NULL,'Ocean Lakes Discharge',NULL,NULL);
INSERT INTO "platform" VALUES(45,NULL,NULL,1,NULL,'WAC-030','dhec.WAC-030.monitorstation',-78.961151,33.616112,1,NULL,NULL,NULL,NULL,NULL,'16th Ave N, Surfside',NULL,NULL);
INSERT INTO "platform" VALUES(46,NULL,NULL,1,NULL,'WAC-031','dhec.WAC-031.monitorstation',-78.964134,33.613144,1,NULL,NULL,NULL,NULL,NULL,'11th Ave N, Surfside',NULL,NULL);
INSERT INTO "platform" VALUES(47,NULL,NULL,1,NULL,'WAC-031A','dhec.WAC-031A.monitorstation',-78.967896,33.608765,1,NULL,NULL,NULL,NULL,NULL,'5th Ave N Swash, Surfside',NULL,NULL);
INSERT INTO "platform" VALUES(48,NULL,NULL,1,NULL,'WAC-032','dhec.WAC-032.monitorstation',-78.969139,33.60775,1,NULL,NULL,NULL,NULL,NULL,'3rd Ave N, Surfside',NULL,NULL);
INSERT INTO "platform" VALUES(49,NULL,NULL,1,NULL,'WAC-033','dhec.WAC-033.monitorstation',-78.974052,33.602741,1,NULL,NULL,NULL,NULL,NULL,'3rd Ave S, Surfside',NULL,NULL);
INSERT INTO "platform" VALUES(50,NULL,NULL,1,NULL,'WAC-034','dhec.WAC-034.monitorstation',-78.977165,33.599388,1,NULL,NULL,NULL,NULL,NULL,'8th Ave S, Surfside',NULL,NULL);
INSERT INTO "platform" VALUES(51,NULL,NULL,1,NULL,'WAC-035','dhec.WAC-035.monitorstation',-78.981026,33.595291,1,NULL,NULL,NULL,NULL,NULL,'13th Ave S, Surfside',NULL,NULL);
INSERT INTO "platform" VALUES(52,NULL,NULL,1,NULL,'WAC-036','dhec.WAC-036.monitorstation',-78.987549,33.588184,1,NULL,NULL,NULL,NULL,NULL,'Hawes Ave, Garden City',NULL,NULL);
INSERT INTO "platform" VALUES(53,NULL,NULL,1,NULL,'WAC-037','dhec.WAC-037.monitorstation',-78.998749,33.57597,1,NULL,NULL,NULL,NULL,NULL,'Azalea Ace, Garden City',NULL,NULL);
INSERT INTO "platform" VALUES(54,NULL,NULL,1,NULL,'WAC-038','dhec.WAC-038.monitorstation',-79.028046,33.534336,1,NULL,NULL,NULL,NULL,NULL,'Garden City Point',NULL,NULL);
INSERT INTO "platform" VALUES(55,NULL,NULL,1,NULL,'WAC-039','dhec.WAC-039.monitorstation',-79.048546,33.514484,1,NULL,NULL,NULL,NULL,NULL,'North Access HB State Park',NULL,NULL);
INSERT INTO "platform" VALUES(56,NULL,NULL,1,NULL,'WAC-040','dhec.WAC-040.monitorstation',-79.065063,33.501568,1,NULL,NULL,NULL,NULL,NULL,'Visitors Center HB State Park',NULL,NULL);
INSERT INTO "platform" VALUES(57,NULL,NULL,1,NULL,'WAC-041','dhec.WAC-041.monitorstation',-79.082657,33.48526,1,NULL,NULL,NULL,NULL,NULL,'Songbird Lane, Litchfield Beach',NULL,NULL);
INSERT INTO "platform" VALUES(58,NULL,NULL,1,NULL,'WAC-042','dhec.WAC-042.monitorstation',-79.09568,33.469109,1,NULL,NULL,NULL,NULL,NULL,'Litchfield Inn',NULL,NULL);
INSERT INTO "platform" VALUES(59,NULL,NULL,1,NULL,'WAC-043A','dhec.WAC-043A.monitorstation',-79.100632,33.461849,1,NULL,NULL,NULL,NULL,NULL,'1st L Past Gate',NULL,NULL);
INSERT INTO "platform" VALUES(60,NULL,NULL,1,NULL,'WAC-044A','dhec.WAC-044A.monitorstation',-79.116087,33.437032,1,NULL,NULL,NULL,NULL,NULL,'Public Access 2nd - Atlantic',NULL,NULL);
INSERT INTO "platform" VALUES(61,NULL,NULL,1,NULL,'WAC-045A','dhec.WAC-045A.monitorstation',-79.130806,33.412083,1,NULL,NULL,NULL,NULL,NULL,'Public Access Springs/ha PI',NULL,NULL);
INSERT INTO "platform" VALUES(62,NULL,NULL,1,NULL,'WAC-046','dhec.WAC-046.monitorstation',-79.138123,33.399624,1,NULL,NULL,NULL,NULL,NULL,'Pawleys Is. South Parking',NULL,NULL);
INSERT INTO "platform" VALUES(63,NULL,NULL,1,NULL,'WAC-047','dhec.WAC-047.monitorstation',-79.148521,33.375084,1,NULL,NULL,NULL,NULL,NULL,'Luvan Way, Debordieu',NULL,NULL);
INSERT INTO "platform" VALUES(64,NULL,NULL,1,NULL,'WAC-048','dhec.WAC-048.monitorstation',-79.151688,33.359783,1,NULL,NULL,NULL,NULL,NULL,'Lafayette / Ocean Green Blvd, Debordieu',NULL,NULL);

DROP TABLE IF EXISTS "sensor";
CREATE TABLE sensor (
    row_id integer PRIMARY KEY,
    row_entry_date timestamp,
    row_update_date timestamp,
    platform_id integer NOT NULL,
    type_id integer,
    short_name varchar(50),
    m_type_id integer NOT NULL,
    fixed_z double precision,
    active integer,
    begin_date timestamp,
    end_date timestamp,
    s_order integer NOT NULL default 1,
    url varchar(200),
    metadata_id integer,
    report_interval integer
);
INSERT INTO "sensor" VALUES(1,NULL,NULL,1,NULL,'precipitation',66,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(2,NULL,NULL,1,NULL,'precipitation_accumulated_daily',67,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(3,NULL,NULL,2,NULL,'precipitation',66,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(4,NULL,NULL,2,NULL,'precipitation_accumulated_daily',67,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(5,NULL,NULL,3,NULL,'precipitation',66,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(6,NULL,NULL,3,NULL,'precipitation_accumulated_daily',67,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(7,NULL,NULL,4,NULL,'precipitation',66,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(8,NULL,NULL,4,NULL,'precipitation_accumulated_daily',67,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(9,NULL,NULL,5,NULL,'precipitation',66,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(10,NULL,NULL,5,NULL,'precipitation_accumulated_daily',67,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(11,NULL,NULL,5,NULL,'wind_speed',68,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(12,NULL,NULL,5,NULL,'wind_from_direction',3,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(13,NULL,NULL,6,NULL,'precipitation',66,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(14,NULL,NULL,6,NULL,'precipitation_accumulated_daily',67,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(15,NULL,NULL,6,NULL,'wind_speed',68,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(16,NULL,NULL,6,NULL,'wind_from_direction',3,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(17,NULL,NULL,7,NULL,'precipitation',66,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(18,NULL,NULL,7,NULL,'precipitation_accumulated_daily',67,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(19,NULL,NULL,8,NULL,'precipitation',66,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(20,NULL,NULL,8,NULL,'precipitation_accumulated_daily',67,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(21,NULL,NULL,9,NULL,'precipitation',66,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(22,NULL,NULL,9,NULL,'precipitation_accumulated_daily',67,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(23,NULL,NULL,10,NULL,'precipitation',66,0,1,NULL,NULL,1,NULL,NULL,NULL);
INSERT INTO "sensor" VALUES(24,NULL,NULL,10,NULL,'precipitation_accumulated_daily',67,0,1,NULL,NULL,1,NULL,NULL,NULL);

