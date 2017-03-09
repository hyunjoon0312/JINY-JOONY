CREATE TABLE NEOKusers(
	NEOKid VARCHAR(12) NOT NULL,
	NEOKpasswd VARCHAR(255) NOT NULL,
	NEOKname VARCHAR(20) NOT NULL,
	NEOKemail VARCHAR(30) NOT NULL,
	PRIMARY KEY(NEOKid)
	);
	
	INSERT INTO NEOKusers VALUES ('neoktest', password('neoktest'), 'joon', 'joon@gmail.com');