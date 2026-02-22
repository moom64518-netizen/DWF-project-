--this script creates a new database named DWH wihtin three layers bronze-silver-gold

--first create db then create schemas 

CREATE DATABASE IF NOT EXISTS DWH ;

  create schema bronze;

  create schema silver;

  create schema gold ;
