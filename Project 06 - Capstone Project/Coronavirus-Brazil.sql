CREATE TABLE "coronavirus" (
  "coronavirus_id" int PRIMARY KEY IDENTITY(0,1),
  "date" date,
  "city_id" int,
  "health_region_id" int distkey,
  "region_id" int,
  "state_id" int,
  "cases" float,
  "deaths" float,
  "population" float
)
compound sortkey(state_id,region_id,city_id);

CREATE TABLE "cities" (
  "city_id" int PRIMARY KEY sortkey,
  "city" text,
  "lat" float,
  "long" float,
  "capital" boolean
)
diststyle ALL;

CREATE TABLE "health_region" (
  "health_region_id" int PRIMARY KEY distkey,
  "health_region" varchar sortkey
);

CREATE TABLE "region" (
  "region_id" int PRIMARY KEY sortkey,
  "region" varchar
)
diststyle ALL;

CREATE TABLE "state" (
  "state_id" int PRIMARY KEY, sortkey
  "state" text
)
diststyle ALL;

CREATE TABLE "time" (
  "date_id" date PRIMARY KEY sortkey,
  "day" int,
  "week" int,
  "month" int,
  "year" int,
  "weekday" int
)
diststyle ALL;

ALTER TABLE "coronavirus" ADD FOREIGN KEY ("date") REFERENCES "time" ("date_id");

ALTER TABLE "coronavirus" ADD FOREIGN KEY ("city_id") REFERENCES "cities" ("city_id");

ALTER TABLE "coronavirus" ADD FOREIGN KEY ("health_region_id") REFERENCES "health_region" ("health_region_id");

ALTER TABLE "coronavirus" ADD FOREIGN KEY ("region_id") REFERENCES "region" ("region_id");

ALTER TABLE "coronavirus" ADD FOREIGN KEY ("state_id") REFERENCES "state" ("state_id");

CREATE INDEX ON "coronavirus" ("coronavirus_id");

CREATE INDEX ON "cities" ("city_id");

CREATE INDEX ON "health_region" ("health_region_id");

CREATE INDEX ON "region" ("region_id");

CREATE INDEX ON "state" ("state_id");

CREATE INDEX ON "time" ("date_id");
