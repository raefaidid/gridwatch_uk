
--create table
CREATE TABLE gridwatch AS SELECT * FROM read_csv('data/gridwatch_data_dictionary.csv');

--show table schema
SHOW gridwatch;

--preview table data
SELECT * FROM gridwatch;

-- year-on-year averages, daily/weekly/yearly peak and through demand