CREATE TABLE users (
  userid SERIAL PRIMARY KEY,
  name VARCHAR(50) UNIQUE, 
  username VARCHAR(50)
)
;

CREATE FUNCTION ConcatenateStrings (firststring varchar(100), secondstring varchar(100))
RETURNS VARCHAR AS
$body$ 
BEGIN
  RETURN COALESCE(firststring, '')  || ' ' || COALESCE(secondstring, '');
END;
$body$
LANGUAGE plpgsql
;

CREATE FUNCTION CalcLength(name varchar(100)) RETURNS INTEGER AS 
$body$
BEGIN
  RETURN LENGTH(name);
END;
$body$
LANGUAGE plpgsql
;

CREATE FUNCTION ConcatenateF (firststring VARCHAR(100), secondstring varchar(100)) 
RETURNS VARCHAR(200) AS
$body$
BEGIN
  RETURN COALESCE(firststring, '') || ' ' || COALESCE(secondstring, '');
END;
$body$
LANGUAGE plpgsql
;

CREATE FUNCTION makeuser() RETURNS VOID AS 
$body$
BEGIN
  INSERT INTO users (name, username) 
       VALUES ('user1','fromproc');
END;
$body$
LANGUAGE plpgsql
;

CREATE FUNCTION createuser(newname varchar(100), newusername varchar(100)) 
RETURNS VOID AS
$body$
BEGIN
  INSERT INTO users (name, username) 
       VALUES (newname, newusername);
END;
$body$
LANGUAGE plpgsql
;

CREATE FUNCTION Multiply(factor INTEGER, val INTEGER) 
RETURNS INTEGER AS
$body$
BEGIN
  RETURN (val*factor);
END;
$body$
LANGUAGE plpgsql
;

CREATE FUNCTION raise_error_no_params() 
RETURNS VOID AS
$body$
BEGIN
  RAISE EXCEPTION SQLSTATE '20001';
END;
$body$
LANGUAGE plpgsql
;

CREATE FUNCTION raise_error_with_params(name VARCHAR, OUT strlength INTEGER) 
RETURNS INTEGER AS
$body$
BEGIN
  IF name = 'xx' THEN
    RAISE EXCEPTION SQLSTATE '20001';
  END IF;
  strlength = CHAR_LENGTH(name);
END;
$body$
LANGUAGE plpgsql
;


