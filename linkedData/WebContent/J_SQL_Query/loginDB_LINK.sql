CREATE TABLE LINKusers(
	LINKid VARCHAR(12) NOT NULL,
	LINKpasswd VARCHAR(255) NOT NULL,
	LINKname VARCHAR(20) NOT NULL,
	LINKemail VARCHAR(30) NOT NULL,
	PRIMARY KEY(LINKid)
	);
	
	INSERT INTO LINKusers VALUES ('linktest', password('linktest'), 'joon', 'joon@gmail.com');