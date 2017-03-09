CREATE TABLE INusers(
	INid VARCHAR(12) NOT NULL,
	INpasswd VARCHAR(255) NOT NULL,
	INname VARCHAR(20) NOT NULL,
	INemail VARCHAR(30) NOT NULL,
	PRIMARY KEY(INid)
	);
	
	INSERT INTO INusers VALUES ('intest', password('intest'), 'joon', 'joon@gmail.com');