create table users(name varchar(50) unique, username varchar(50), userid int auto_increment primary key) ENGINE=InnoDB;

CREATE PROCEDURE ConcatenateStrings (IN firststring varchar(100), IN secondstring varchar(100), OUT concatenated varchar(200)) set concatenated = concat(firststring , concat( ' ' , secondstring ));

create procedure CalcLength(IN name varchar(100), OUT strlength int) set strlength =length(name);

CREATE FUNCTION ConcatenateF (firststring  VARCHAR(100), secondstring varchar(100)) RETURNS VARCHAR(200) DETERMINISTIC RETURN CONCAT(firststring,' ',secondstring);

create procedure makeuser() insert into users (name,username) values ('user1','fromproc');

create procedure createuser(IN newname varchar(100), IN newusername varchar(100)) insert into users (name,username) values (newname, newusername);

create procedure Multiply(IN factor int, INOUT val int) set val =val*factor;

CREATE PROCEDURE raise_error_no_params() BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'test exception', MYSQL_ERRNO = 20001; END;

CREATE PROCEDURE raise_error_with_params(IN name VARCHAR, OUT strlength INTEGER) BEGIN IF (name = 'xx') THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'test exception', MYSQL_ERRNO = 20001; END IF; SET strlength = LENGTH(name); END;
